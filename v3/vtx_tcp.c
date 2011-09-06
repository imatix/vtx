/*  =====================================================================
    VTX - 0MQ virtual transport interface - ZMTP / TCP driver

    Implements the VTX virtual socket interface using the ZMTP protocol
    over TCP. This is for sanity testing with existing 0MQ applications.

        zmtp        = *connection

        connection  = greeting content
        greeting    = anonymous / identity
        anonymous   = %x01 final
        identity    = length final (%x01-ff) *OCTET

        message     = *more-frame final-frame
        more-frame  = length more body
        final-frame = length final body
        length      = OCTET / (%xFF 8OCTET)
        more        = %x01
        final       = %x00
        body        = *OCTET

        content     = *broadcast / *addressed / *neutral

        broadcast   = message

        addressed   = envelope message
        envelope    = *more-frame delimiter
        delimiter   = %x01 more

        neutral     = message

    ---------------------------------------------------------------------
    Copyright (c) 1991-2011 iMatix Corporation <www.imatix.com>
    Copyright other contributors as noted in the AUTHORS file.

    This is free software; you can redistribute it and/or modify it under
    the terms of the GNU Lesser General Public License as published by
    the Free Software Foundation; either version 3 of the License, or (at
    your option) any later version.

    This software is distributed in the hope that it will be useful, but
    WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
    Lesser General Public License for more details.

    You should have received a copy of the GNU Lesser General Public
    License along with this program. If not, see
    <http://www.gnu.org/licenses/>.
    =====================================================================
*/

#include "vtx_tcp.h"
#include "vtx_codec.c"

//  Report a fatal error and exit the program without cleaning up
//  Use of derp() should be gradually reduced to real failures.
static void derp (char *s) { perror (s); exit (1); }

#define IN_ADDR_SIZE    sizeof (struct sockaddr_in)
#define VOCKET_STATS    0
#undef  VOCKET_STATS

//  ---------------------------------------------------------------------
//  These are the objects we play with in our driver

typedef struct _driver_t driver_t;
typedef struct _vocket_t vocket_t;
typedef struct _binding_t binding_t;
typedef struct _peering_t peering_t;


//  ---------------------------------------------------------------------
//  A driver_t holds the context for one driver thread, which matches
//  one registered driver. We create a driver by calling vtx_tcp_driver,
//  and the thread runs until the process is interrupted. A driver works
//  with a list of vockets, which are virtual 0MQ sockets.

struct _driver_t {
    zctx_t *ctx;                //  Own context
    char *scheme;               //  Driver scheme
    zloop_t *loop;              //  zloop reactor for socket I/O
    zlist_t *vockets;           //  List of vockets per driver
    void *pipe;                 //  Control pipe to/from VTX frontend
    Bool verbose;               //  Trace activity?
};

//  A vocket_t holds the context for one virtual socket, which implements
//  the semantics of a 0MQ socket. We create a vocket when first binding
//  or connecting a VTX name. We destroy vockets at shutdown, or when the
//  caller calls vtx_close. A vocket manages a set of bindings for
//  incoming connections, a set of peerings to other nodes, and other
//  transport-specific properties.
//

struct _vocket_t {
    driver_t *driver;           //  Parent driver object
    char *vtxname;              //  Message pipe VTX address
    void *msgpipe;              //  Message pipe (0MQ socket)
    zhash_t *binding_hash;      //  Bindings, indexed by address
    zhash_t *peering_hash;      //  Peerings, indexed by address
    zlist_t *peering_list;      //  Peerings, in simple list
    zlist_t *live_peerings;     //  Peerings that are alive
    Bool more;                  //  More parts of message expected
    peering_t *current_peering; //  Peering that is receiving message
    uint peerings;              //  Current number of peerings
    //  Vocket metadata, available via getmeta call
    char sender [16];           //  Address of last message sender
    //  These properties control the vocket routing semantics
    uint routing;               //  Routing mechanism
    Bool nomnom;                //  Accepts incoming messages
    uint min_peerings;          //  Minimum peerings for routing
    uint max_peerings;          //  Maximum allowed peerings
    //  hwm strategy
    //  filter on input messages
    //  ZMTP specific properties
    uint inbuf_max;             //  Input codec buffer limit
    uint outbuf_max;            //  Output codec buffer limit
    //  Statistics and reporting
    int socktype;               //  0MQ socket type
    uint outgoing;              //  Messages sent
    uint incoming;              //  Messages received
    uint outpiped;              //  Messages sent from pipe
    uint inpiped;               //  Messages sent to pipe
    uint dropped;               //  Incoming messages dropped
};

//  This maps 0MQ socket types to the VTX emulation
static struct {
    int socktype;
    int routing;
    Bool nomnom;
    int min_peerings;
    int max_peerings;
} s_vocket_config [] = {
    { ZMQ_REQ,    VTX_ROUTING_REQUEST, TRUE,  1, VTX_MAX_PEERINGS },
    { ZMQ_REP,    VTX_ROUTING_REPLY,   TRUE,  1, VTX_MAX_PEERINGS },
    { ZMQ_ROUTER, VTX_ROUTING_ROUTER,  TRUE,  0, VTX_MAX_PEERINGS },
    { ZMQ_DEALER, VTX_ROUTING_DEALER,  TRUE,  1, VTX_MAX_PEERINGS },
    { ZMQ_PUB,    VTX_ROUTING_PUBLISH, FALSE, 0, VTX_MAX_PEERINGS },
    { ZMQ_SUB,    VTX_ROUTING_NONE,    TRUE,  1, VTX_MAX_PEERINGS },
    { ZMQ_PUSH,   VTX_ROUTING_DEALER,  FALSE, 1, VTX_MAX_PEERINGS },
    { ZMQ_PULL,   VTX_ROUTING_NONE,    TRUE,  1, VTX_MAX_PEERINGS },
    { ZMQ_PAIR,   VTX_ROUTING_SINGLE,  TRUE,  1, 1 }
};


//  A binding_t holds the context for a single binding.
//  For ZMTP, this is includes the native TCP socket handle.

struct _binding_t {
    driver_t *driver;           //  Parent driver object
    vocket_t *vocket;           //  Parent vocket object
    char *address;              //  Local address:port bound to
    Bool exception;             //  Binding could not be initialized
    //  ZMTP specific properties
    int handle;                 //  TCP socket handle
};

//  A peering_t holds the context for a peering to another node across
//  our transport. Peerings can be outgoing (will try to reconnect if
//  lowered) or incoming (will be destroyed when lowered).
//  For ZMTP, this includes the actual TCP address to talk to,
//  and the broadcast address if this was a broadcast connection.

struct _peering_t {
    driver_t *driver;           //  Parent driver object
    vocket_t *vocket;           //  Parent vocket object
    Bool alive;                 //  Is peering raised and alive?
    Bool outgoing;              //  Connected handles?
    Bool subscribed;            //  Is peering receiving messages (SUB only)
    char *address;              //  Peer address as nnn.nnn.nnn.nnn:nnnnn
    Bool exception;             //  Peering could not be initialized
    vtx_codec_t *input;         //  Input message queue
    vtx_codec_t *output;        //  Output message queue
    Bool more;                  //  Waiting for more message parts
    //  ZMTP specific properties
    int handle;                 //  Handle for input/output
    int interval;               //  Current reconnect interval
    int events;                 //  Current poll events
    struct sockaddr_in addr;    //  Peer address as sockaddr_in
};

//  Basic methods for each of our object types (it's not really a clean
//  abstraction since objects are not opaque, but it works pretty well.)
//
static driver_t *
    driver_new (zctx_t *ctx, void *pipe);
static void
    driver_destroy (driver_t **self_p);
static vocket_t *
    vocket_new (driver_t *driver, int socktype, char *vtxname);
static void
    vocket_destroy (vocket_t **self_p);
static binding_t *
    binding_require (vocket_t *vocket, char *address);
static void
    binding_delete (void *argument);
static peering_t *
    peering_require (vocket_t *vocket, char *address, Bool outgoing);
static void
    peering_destroy (peering_t **self_p);
static void
    peering_delete (void *argument);
static void
    peering_raise (peering_t *self);
static void
    peering_lower (peering_t *self);
static void
    peering_poller (peering_t *self, int events);

//  Reactor handlers
static int
    s_driver_control (zloop_t *loop, zmq_pollitem_t *item, void *arg);
static int
    s_vocket_input (zloop_t *loop, zmq_pollitem_t *item, void *arg);
static int
    s_binding_input (zloop_t *loop, zmq_pollitem_t *item, void *arg);
static int
    s_peering_activity (zloop_t *loop, zmq_pollitem_t *item, void *arg);
static int
    s_peering_monitor (zloop_t *loop, zmq_pollitem_t *item, void *arg);

//  Utility functions
static void
    s_queue_output (peering_t *self, zmq_msg_t *msg, Bool more);
static void
    s_send_wire (peering_t *self);
static ssize_t
    s_recv_wire (peering_t *self);
static char *
    s_sin_addr_to_str (struct sockaddr_in *addr);
static int
    s_str_to_sin_addr (struct sockaddr_in *addr, char *address);
static void
    s_set_nonblock (int handle);
static void
    s_close_handle (int handle, driver_t *driver);
static int
    s_handle_io_error (char *reason);

//  ---------------------------------------------------------------------
//  Main driver thread is minimal, all work is done by reactor

void vtx_tcp_driver (void *args, zctx_t *ctx, void *pipe)
{
    //  Create driver instance
    driver_t *driver = driver_new (ctx, pipe);
    char *verbose = zstr_recv (pipe);
    driver->verbose = atoi (verbose);
    free (verbose);
    zloop_set_verbose (driver->loop, driver->verbose);
    //  Run reactor until we exit from failure or interrupt
    zloop_start (driver->loop);
    //  Destroy driver instance
    driver_destroy (&driver);
}

//  ---------------------------------------------------------------------
//  Registers our protocol driver with the VTX engine

int vtx_tcp_load (vtx_t *vtx, Bool verbose)
{
    return vtx_register (vtx, VTX_TCP_SCHEME, vtx_tcp_driver, verbose);
}


//  ---------------------------------------------------------------------
//  Constructor and destructor for driver

static driver_t *
driver_new (zctx_t *ctx, void *pipe)
{
    driver_t *self = (driver_t *) zmalloc (sizeof (driver_t));
    self->ctx = ctx;
    self->pipe = pipe;
    self->vockets = zlist_new ();
    self->loop = zloop_new ();
    self->scheme = VTX_TCP_SCHEME;

    //  Reactor starts by monitoring the driver control pipe
    zmq_pollitem_t item = { self->pipe, 0, ZMQ_POLLIN };
    zloop_poller (self->loop, &item, s_driver_control, self);
    return self;
}

static void
driver_destroy (driver_t **self_p)
{
    assert (self_p);
    if (*self_p) {
        driver_t *self = *self_p;
        if (self->verbose)
            zclock_log ("I: (tcp) shutting down driver");
        while (zlist_size (self->vockets)) {
            vocket_t *vocket = (vocket_t *) zlist_pop (self->vockets);
            vocket_destroy (&vocket);
        }
        zlist_destroy (&self->vockets);
        zloop_destroy (&self->loop);
        free (self);
        *self_p = NULL;
    }
}


//  ---------------------------------------------------------------------
//  Constructor and destructor for vocket

static vocket_t *
vocket_new (driver_t *driver, int socktype, char *vtxname)
{
    assert (driver);
    vocket_t *self = (vocket_t *) zmalloc (sizeof (vocket_t));

    self->driver = driver;
    self->vtxname = strdup (vtxname);
    self->binding_hash = zhash_new ();
    self->peering_hash = zhash_new ();
    self->peering_list = zlist_new ();
    self->live_peerings = zlist_new ();
    self->socktype = socktype;

    uint index;
    for (index = 0; index < tblsize (s_vocket_config); index++)
        if (socktype == s_vocket_config [index].socktype)
            break;

    if (index < tblsize (s_vocket_config)) {
        self->routing = s_vocket_config [index].routing;
        self->nomnom = s_vocket_config [index].nomnom;
        self->min_peerings = s_vocket_config [index].min_peerings;
        self->max_peerings = s_vocket_config [index].max_peerings;
    }
    else {
        zclock_log ("E: invalid vocket type %d", socktype);
        exit (1);
    }
    //  Create msgpipe vocket and connect over inproc to vtxname
    self->msgpipe = zsocket_new (driver->ctx, ZMQ_PAIR);
    assert (self->msgpipe);
    zsocket_connect (self->msgpipe, "inproc://%s", vtxname);

    //  If we drop on no peerings, start routing input now
    if (self->min_peerings == 0) {
        //  Ask reactor to start monitoring vocket's msgpipe pipe
        zmq_pollitem_t item = { self->msgpipe, 0, ZMQ_POLLIN, 0 };
        zloop_poller (driver->loop, &item, s_vocket_input, self);
    }
    //  Store this vocket per driver so that driver can cleanly destroy
    //  all its vockets when it is destroyed.
    zlist_push (driver->vockets, self);

    //* Start transport-specific work
    self->inbuf_max = VTX_TCP_INBUF_MAX;
    self->outbuf_max = VTX_TCP_OUTBUF_MAX;
    //* End transport-specific work

    return self;
}

static void
vocket_destroy (vocket_t **self_p)
{
    assert (self_p);
    if (*self_p) {
        vocket_t *self = *self_p;
        driver_t *driver = self->driver;

        if (self->min_peerings == 0) {
            //  Ask reactor to stop monitoring vocket's msgpipe
            zmq_pollitem_t item = { self->msgpipe, 0, ZMQ_POLLIN, 0 };
            zloop_poller_end (driver->loop, &item);
        }

        //  Close message msgpipe socket
        zsocket_destroy (driver->ctx, self->msgpipe);

        //  Destroy all bindings for this vocket
        zhash_destroy (&self->binding_hash);

        //  Destroy all peerings for this vocket
        zhash_destroy (&self->peering_hash);
        zlist_destroy (&self->peering_list);
        zlist_destroy (&self->live_peerings);

        //  Remove vocket from driver list of vockets
        zlist_remove (driver->vockets, self);

#ifdef VOCKET_STATS
        char *type_name [] = {
            "PAIR", "PUB", "SUB", "REQ", "REP",
            "DEALER", "ROUTER", "PULL", "PUSH",
            "XPUB", "XSUB"
        };
        printf ("I: type=%s sent=%d recd=%d outp=%d inp=%d drop=%d\n",
            type_name [self->socktype],
            self->outgoing, self->incoming,
            self->outpiped, self->inpiped,
            self->dropped);
#endif
        free (self->vtxname);
        free (self);
        *self_p = NULL;
    }
}

//  ---------------------------------------------------------------------
//  Constructor and destructor for binding
//  Bindings are held per vocket, indexed by peer hostname:port

static binding_t *
binding_require (vocket_t *vocket, char *address)
{
    assert (vocket);
    binding_t *self = (binding_t *) zhash_lookup (vocket->binding_hash, address);

    if (self == NULL) {
        //  Create new binding for this hostname:port address
        self = (binding_t *) zmalloc (sizeof (binding_t));
        self->vocket = vocket;
        self->driver = vocket->driver;
        self->address = strdup (address);
        driver_t *driver = self->driver;

        //  Split port number off address
        char *port = strchr (address, ':');
        assert (port);
        *port++ = 0;

        //* Start transport-specific work
        //  Create new bound TCP socket handle
        self->handle = socket (AF_INET, SOCK_STREAM, IPPROTO_TCP);
        if (self->handle == -1)
            derp ("socket");

        //  Get sockaddr_in structure for address
        struct sockaddr_in addr;
        addr.sin_family = AF_INET;
        addr.sin_port = htons (atoi (port));

        //  Bind handle to specific local address, or *
        if (streq (address, "*"))
            addr.sin_addr.s_addr = htonl (INADDR_ANY);
        else
        if (inet_aton (address, &addr.sin_addr) == 0) {
            zclock_log ("E: bind failed: invalid address '%s'", address);
            self->exception = TRUE;
        }
        if (!self->exception) {
#           ifndef __WINDOWS__
            //  On POSIX systems we need to set SO_REUSEADDR to reuse an
            //  address without a 5-minute timeout. On win32 this option
            //  lets you bind to an in-use address, so we do not do that.
            int reuse = 1;
            setsockopt (self->handle, SOL_SOCKET, SO_REUSEADDR,
                (void *) &reuse, sizeof (reuse));
#           endif
            if (bind (self->handle,
                (const struct sockaddr *) &addr, IN_ADDR_SIZE) == -1) {
                zclock_log ("E: bind failed: '%s'", strerror (errno));
                self->exception = TRUE;
            }
            else
            if (listen (self->handle, VTX_TCP_BACKLOG)) {
                zclock_log ("E: listen failed: '%s'", strerror (errno));
                self->exception = TRUE;
            }
        }
        if (self->exception)
            close (self->handle);
        else {
            //  Ask reactor to start monitoring this binding handle
            zmq_pollitem_t item = { NULL, self->handle, ZMQ_POLLIN, 0 };
            zloop_poller (driver->loop, &item, s_binding_input, vocket);
        }
        //* End transport-specific work
        if (self->exception) {
            free (self->address);
            free (self);
            self = NULL;
        }
        else {
            //  Store new binding in vocket containers
            zhash_insert (vocket->binding_hash, address, self);
            zhash_freefn (vocket->binding_hash, address, binding_delete);
            if (driver->verbose)
                zclock_log ("I: (tcp) create binding to %s", self->address);
        }
    }
    return self;
}

//  Destroy binding object, when binding is removed from vocket->binding_hash

static void
binding_delete (void *argument)
{
    binding_t *self = (binding_t *) argument;
    if (self->driver->verbose)
        zclock_log ("I: (tcp) delete binding %s", self->address);

    //* Start transport-specific work
    s_close_handle (self->handle, self->driver);
    //* End transport-specific work

    free (self->address);
    free (self);
}

//  ---------------------------------------------------------------------
//  Constructor and destructor for peering
//  Peerings are held per vocket, indexed by peer hostname:port

static peering_t *
peering_require (vocket_t *vocket, char *address, Bool outgoing)
{
    assert (vocket);
    peering_t *self = (peering_t *) zhash_lookup (vocket->peering_hash, address);

    if (self == NULL) {
        //  Create new peering for this hostname:port address
        self = (peering_t *) zmalloc (sizeof (peering_t));
        self->vocket = vocket;
        self->driver = vocket->driver;
        self->address = strdup (address);
        self->outgoing = outgoing;
        if (self->driver->verbose)
            zclock_log ("I: (tcp) create peering to %s", address);

        //* Start transport-specific work
        if (self->outgoing) {
            self->interval = VTX_TCP_RECONNECT_IVL;
            s_peering_monitor (self->driver->loop, NULL, self);
        }
        //  Initialize message buffering codecs
        if (!self->exception) {
            self->input = vtx_codec_new (vocket->inbuf_max);
            self->output = vtx_codec_new (vocket->outbuf_max);
        }
        //* End transport-specific work

        if (self->exception) {
            free (self->address);
            free (self);
            self = NULL;
        }
        else {
            //  Store new peering in vocket containers
            zhash_insert (vocket->peering_hash, address, self);
            zhash_freefn (vocket->peering_hash, address, peering_delete);
            zlist_append (vocket->peering_list, self);
            vocket->peerings++;
        }
    }
    return self;
}

//  Destroy peering, indirect by removing from peering hash

static void
peering_destroy (peering_t **self_p)
{
    assert (self_p);
    if (*self_p) {
        peering_t *self = *self_p;
        //  All destruction is done in delete method
        zhash_delete (self->vocket->peering_hash, self->address);
    }
}

//  Destroy peering, when it's removed from vocket->peering_hash

static void
peering_delete (void *argument)
{
    peering_t *self = (peering_t *) argument;
    vocket_t *vocket = self->vocket;
    driver_t *driver = self->driver;
    if (driver->verbose)
        zclock_log ("I: (tcp) delete peering %s", self->address);

    //* Start transport-specific work
    s_close_handle (self->handle, driver);
    //* End transport-specific work

    vtx_codec_destroy (&self->input);
    vtx_codec_destroy (&self->output);
    peering_lower (self);
    zlist_remove (vocket->peering_list, self);
    zloop_timer_end (driver->loop, self);
    free (self->address);
    free (self);
    vocket->peerings--;
}

//  Peering is now active

static void
peering_raise (peering_t *self)
{
    vocket_t *vocket = self->vocket;
    driver_t *driver = self->driver;
    if (driver->verbose)
        zclock_log ("I: (tcp) bring up peering to %s", self->address);

    if (!self->alive) {
        self->alive = TRUE;
        zlist_append (vocket->live_peerings, self);

        //  Send ZMTP handshake, which is an empty message
        zmq_msg_t msg;
        zmq_msg_init_size (&msg, 0);
        s_queue_output (self, &msg, FALSE);

        //  If we can now route to peerings, start reading from msgpipe
        if (zlist_size (vocket->live_peerings) == vocket->min_peerings) {
            //  Ask reactor to start monitoring vocket's msgpipe pipe
            zmq_pollitem_t item = { vocket->msgpipe, 0, ZMQ_POLLIN, 0 };
            zloop_poller (driver->loop, &item, s_vocket_input, vocket);
        }
    }
}

//  Peering is now inactive

static void
peering_lower (peering_t *self)
{
    vocket_t *vocket = self->vocket;
    driver_t *driver = self->driver;
    if (driver->verbose)
        zclock_log ("I: (tcp) take down peering to %s", self->address);
    if (self->alive) {
        self->alive = FALSE;
        zlist_remove (vocket->live_peerings, self);
        if (zlist_size (vocket->live_peerings) < vocket->min_peerings) {
            //  Ask reactor to stop monitoring vocket's msgpipe pipe
            zmq_pollitem_t item = { vocket->msgpipe, 0, ZMQ_POLLIN, 0 };
            zloop_poller_end (driver->loop, &item);
        }
        //  current_peering cannot receive messages any more
        if (vocket->current_peering == self)
            vocket->current_peering = NULL;
        //  Make sure we are not subscribed anymore (makes sense for SUB
        //  socket only)
        self->subscribed = FALSE;
    }
}

//  Reset poller on peering handle, to specified events

static void
peering_poller (peering_t *self, int events)
{
    driver_t *driver = self->driver;
    if (self->events != events) {
        zmq_pollitem_t item = { NULL, self->handle, events, 0 };
        zloop_poller_end (driver->loop, &item);
        if (events)
            zloop_poller (driver->loop, &item, s_peering_activity, self);
        self->events = events;
    }
}


//  ---------------------------------------------------------------------
//  Reactor handlers

//  Handle bind/connect from caller:
//
//  [command]   BIND, CONNECT, GETMETA, CLOSE, SHUTDOWN
//  [socktype]  0MQ socket type as ASCII number
//  [vtxname]   VTX name for the 0MQ socket
//  [address]   External address to bind/connect to, or meta name

static int
s_driver_control (zloop_t *loop, zmq_pollitem_t *item, void *arg)
{
    int rc = 0;
    char *reply = "0";
    driver_t *driver = (driver_t *) arg;
    zmsg_t *request = zmsg_recv (item->socket);

    char *command  = zmsg_popstr (request);
    char *socktype = zmsg_popstr (request);
    char *vtxname  = zmsg_popstr (request);
    char *address  = zmsg_popstr (request);
    zmsg_destroy (&request);

    //  Lookup vocket with this vtxname, create if necessary
    vocket_t *vocket = NULL;
    if (vtxname) {
        vocket = (vocket_t *) zlist_first (driver->vockets);
        while (vocket) {
            if (streq (vocket->vtxname, vtxname))
                break;
            vocket = (vocket_t *) zlist_next (driver->vockets);
        }
        if (!vocket)
            vocket = vocket_new (driver, atoi (socktype), vtxname);
    }
    //  Multiple binds or connects to same address are idempotent
    if (streq (command, "BIND")) {
        assert (vocket);
        if (!binding_require (vocket, address))
            reply = "1";
    }
    else
    if (streq (command, "CONNECT")) {
        assert (vocket);
        if (vocket->peerings < vocket->max_peerings)
            peering_require (vocket, address, TRUE);
        else {
            zclock_log ("E: connect failed: too many peerings");
            reply = "1";
        }
    }
    else
    if (streq (command, "GETMETA")) {
        assert (vocket);
        if (streq (address, "sender"))
            reply = vocket->sender;
        else
            reply = "Unknown name";
    }
    else
    if (streq (command, "CLOSE")) {
        assert (vocket);
        vocket_destroy (&vocket);
    }
    else
    if (streq (command, "SHUTDOWN"))
        rc = -1;                //  Shutdown driver
    else {
        zclock_log ("E: invalid command: %s", command);
        reply = "1";
    }
    zstr_sendf (item->socket, reply);
    free (command);
    free (socktype);
    free (vtxname);
    free (address);
    return rc;
}


//  -------------------------------------------------------------------------
//  Input message on data pipe from application 0MQ socket

static int
s_vocket_input (zloop_t *loop, zmq_pollitem_t *item, void *arg)
{
    vocket_t *vocket = (vocket_t *) arg;
    driver_t *driver = vocket->driver;

    //  It's remotely possible we just lost a peering, in which case
    //  don't take the message off the pipe, leave it for next time
    if (zlist_size (vocket->live_peerings) < vocket->min_peerings)
        return 0;

    //  Pull message parts off socket
    assert (item->socket == vocket->msgpipe);
    zmq_msg_t msg;
    zmq_msg_init (&msg);

    Bool more = vocket->more;
    int rc = zmq_recvmsg (vocket->msgpipe, &msg, 0);
    while (rc >= 0) {
        vocket->outpiped++;
        Bool first = !more;
        more = zsockopt_rcvmore (vocket->msgpipe);

        //  Route message to active peerings as appropriate
        if (vocket->routing == VTX_ROUTING_NONE)
            zclock_log ("W: send() not allowed - dropping");
        else
        if (vocket->routing == VTX_ROUTING_REQUEST) {
            //  First part of message
            //  Round-robin to next peering
            if (first) {
                vocket->current_peering = (peering_t *) zlist_pop (vocket->live_peerings);
                zlist_append (vocket->live_peerings, vocket->current_peering);
            }
            peering_t *peering = vocket->current_peering;
            if (peering)
                s_queue_output (peering, &msg, more);
        }
        else
        if (vocket->routing == VTX_ROUTING_REPLY) {
            peering_t *peering = vocket->current_peering;
            if (peering)
                s_queue_output (peering, &msg, more);
        }
        else
        if (vocket->routing == VTX_ROUTING_DEALER) {
            //  First part of message
            //  Round-robin to next peering
            if (first) {
                vocket->current_peering = (peering_t *) zlist_pop (vocket->live_peerings);
                zlist_append (vocket->live_peerings, vocket->current_peering);
            }
            peering_t *peering = vocket->current_peering;
            if (peering)
                s_queue_output (peering, &msg, more);
        }
        else
        if (vocket->routing == VTX_ROUTING_ROUTER) {
            peering_t *peering = vocket->current_peering;
            //  Look-up peering using first message part
            if (first) {
                //  Parse and check schemed identity
                size_t size = zmq_msg_size (&msg);
                char *address = (char *) malloc (size + 1);
                memcpy (address, zmq_msg_data (&msg), size);
                address [size] = 0;

                int scheme_size = strlen (driver->scheme);
                if (memcmp (address, driver->scheme, scheme_size) == 0
                &&  memcmp (address + scheme_size, "://", 3) == 0) {
                    peering = (peering_t *) zhash_lookup (
                        vocket->peering_hash, address + scheme_size + 3);
                    if (!peering || !peering->alive)
                        zclock_log ("W: no route to '%s' - dropping", address);
                    vocket->current_peering = peering;
                }
                else
                    zclock_log ("E: bad address '%s' - dropping", address);
                free (address);
            }
            else
                if (peering)
                    s_queue_output (peering, &msg, more);
        }
        else
        if (vocket->routing == VTX_ROUTING_PUBLISH) {
            //  This is the first part of possibly multi-part message
            //  Subscribe all peerings that are alive
            if (first) {
                peering_t *peering = (peering_t *) zlist_first (vocket->live_peerings);
                while (peering) {
                    peering->subscribed = TRUE;
                    peering = (peering_t *) zlist_next (vocket->live_peerings);
                }
            }

            if (zlist_size (vocket->live_peerings) > 1) {
                //  Duplicate frames to all subscribers
                peering_t *peering = (peering_t *) zlist_first (vocket->live_peerings);
                while (peering) {
                    if (peering->subscribed)
                        s_queue_output (peering, &msg, more);
                    peering = (peering_t *) zlist_next (vocket->live_peerings);
                }
            }
            else
            if (zlist_size (vocket->live_peerings) == 1) {
                //  Send frames straight through to single subscriber
                peering_t *peering = (peering_t *) zlist_first (vocket->live_peerings);
                if (peering->subscribed)
                    s_queue_output (peering, &msg, more);
            }
        }
        else
        if (vocket->routing == VTX_ROUTING_SINGLE) {
            if (first)
                vocket->current_peering = (peering_t *) zlist_first (vocket->live_peerings);
            peering_t *peering = vocket->current_peering;
            if (peering)
                s_queue_output (peering, &msg, more);
        }
        else
            zclock_log ("E: unknown routing mechanism - dropping");

        zmq_msg_close (&msg);
        zmq_msg_init (&msg);
        rc = zmq_recvmsg (vocket->msgpipe, &msg, ZMQ_DONTWAIT);
    }
    //  Save state
    vocket->more = more;

    return 0;
}


//  -------------------------------------------------------------------------
//  Queue message for sending to peering, start output poller if necessary
//  so that message will be sent when network is ready for it.

static void
s_queue_output (peering_t *self, zmq_msg_t *msg, Bool more)
{
    assert (self);
    assert (self->alive);
    vtx_codec_msg_put (self->output, msg, more);
    peering_poller (self, ZMQ_POLLIN + ZMQ_POLLOUT);
}


//  -------------------------------------------------------------------------
//  Accept incoming TCP connection request on binding handle
//  Creates a new peering, if successful

static int
s_binding_input (zloop_t *loop, zmq_pollitem_t *item, void *arg)
{
    vocket_t *vocket = (vocket_t *) arg;
    driver_t *driver = vocket->driver;

    struct sockaddr_in addr;        //  Peer address
    socklen_t addr_len = sizeof (addr);

    int handle = accept (item->fd, (struct sockaddr *) &addr, &addr_len);
    if (handle >= 0) {
        s_set_nonblock (handle);
        if (vocket->peerings < vocket->max_peerings) {
            char *address = s_sin_addr_to_str (&addr);
            peering_t *peering = peering_require (vocket, address, FALSE);
            peering->handle = handle;
            peering_raise (peering);
            peering_poller (peering, ZMQ_POLLIN + ZMQ_POLLOUT);
        }
        else {
            zclock_log ("W: Max peerings reached for socket");
            close (handle);
        }
    }
    else
        s_handle_io_error ("accept");

    return 0;
}


//  -------------------------------------------------------------------------
//  Activity on peering handle

static int
s_peering_activity (zloop_t *loop, zmq_pollitem_t *item, void *arg)
{
    peering_t *peering = (peering_t *) arg;
    vocket_t *vocket = peering->vocket;
    driver_t *driver = peering->driver;

    if (peering->alive) {
        if (item->revents & ZMQ_POLLERR) {
            if (driver->verbose)
                zclock_log ("I: (tcp) peering alive/error %s",
                    peering->address);
            peering->exception = TRUE;
        }
        else
        if (item->revents & ZMQ_POLLIN) {
            if (driver->verbose)
                zclock_log ("I: (tcp) peering alive/input %s",
                    peering->address);
            s_recv_wire (peering);
        }
        else
        if (item->revents & ZMQ_POLLOUT) {
            if (driver->verbose)
                zclock_log ("I: (tcp) peering alive/output %s",
                    peering->address);
            s_send_wire (peering);
        }
    }
    else
    if (peering->outgoing) {
        if (item->revents & ZMQ_POLLERR) {
            if (driver->verbose)
                zclock_log ("I: (tcp) peering dead/error %s", peering->address);
            peering->exception = TRUE;
        }
        else
        if (item->revents & ZMQ_POLLIN
        ||  item->revents & ZMQ_POLLOUT) {
            if (driver->verbose)
                zclock_log ("I: (tcp) peering dead/inout %s", peering->address);
            peering_poller (peering, ZMQ_POLLIN);
            peering_raise (peering);
        }
    }
    //  Handle exception peering by switching to monitoring, or killing it
    if (peering->exception) {
        peering_lower (peering);
        if (peering->outgoing) {
            close (peering->handle);
            peering_poller (peering, 0);
            peering->handle = 0;
            zloop_timer (loop, peering->interval, 1, s_peering_monitor, peering);
        }
        else
            peering_destroy (&peering);
    }
    return 0;
}


//  -------------------------------------------------------------------------
//  Monitor peering for connectivity

static int
s_peering_monitor (zloop_t *loop, zmq_pollitem_t *item, void *arg)
{
    peering_t *peering = (peering_t *) arg;
    vocket_t *vocket = peering->vocket;
    driver_t *driver = peering->driver;

    //  The peering monitor handles just outgoing peering reconnect
    //  attempts. It'll keep trying until successful.
    assert (peering->outgoing);
    peering->exception = FALSE;
    if (peering->alive)
        return 0;           //  Stop monitor if peering came alive

    if (driver->verbose)
        zclock_log ("I: (tcp) connecting to '%s'...", peering->address);
    peering->handle = socket (AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (peering->handle == -1) {
        zclock_log ("E: connect failed: no sockets - %s", strerror (errno));
        goto error;
    }
    s_set_nonblock (peering->handle);
    if (s_str_to_sin_addr (&peering->addr, peering->address)) {
        zclock_log ("E: connect failed: bad address '%s'", peering->address);
        goto error;
    }
    int rc = connect (peering->handle,
        (const struct sockaddr *) &peering->addr, IN_ADDR_SIZE);
    if (rc == -1 && errno != EINPROGRESS) {
        zclock_log ("E: connect failed: '%s'", strerror (errno));
        goto error;
    }
    peering_poller (peering, ZMQ_POLLIN + ZMQ_POLLOUT);
    return 0;

error:
    if (peering->handle > 0) {
        close (peering->handle);
        peering->handle = 0;
    }
    //  Try again later
    zloop_timer (loop, peering->interval, 1, s_peering_monitor, peering);
    return 0;
}


//  Send frame data to peering, and handle errors on socket

static void
s_send_wire (peering_t *self)
{
    vocket_t *vocket = self->vocket;
    driver_t *driver = self->driver;

    while (TRUE) {
        byte *data;
        size_t size = vtx_codec_bin_get (self->output, &data);
        if (size == 0) {
            peering_poller (self, ZMQ_POLLIN);
            break;      //  Buffer is empty, stop polling out
        }
        if (driver->verbose)
            zclock_log ("I: (tcp) send %zd bytes to %s",
                size, self->address);
        int bytes_sent = send (self->handle, data, size, 0);
        if (driver->verbose)
            zclock_log ("I: (tcp) actually sent %d bytes", bytes_sent);

        if (bytes_sent > 0) {
            vtx_codec_bin_tick (self->output, bytes_sent);
            if (bytes_sent < size)
                break;      //  Wait until network can accept more
        }
        else
        if (bytes_sent == 0 || s_handle_io_error ("send") == -1) {
            self->exception = TRUE;
            break;          //  Signal error and give up
        }
    }
}


//  Receive frame data from peering, and handle errors on socket

static ssize_t
s_recv_wire (peering_t *self)
{
    vocket_t *vocket = self->vocket;
    driver_t *driver = self->driver;

    //  Read into buffer and dump what we got
    //  TODO: only read as much as we have space in input queue
    //  implement exception strategy here
    //  - drop oldest, drop newest, pushback
    byte buffer [VTX_TCP_BUFSIZE];
    ssize_t size = recv (self->handle, buffer, VTX_TCP_BUFSIZE, MSG_DONTWAIT);
    if (size == 0)
        //  Other side closed TCP socket, so our peering is down
        self->exception = TRUE;
    else
    if (size == -1) {
        if (s_handle_io_error ("recv") == -1)
            //  Hard error on socket, so peering is down
            self->exception = TRUE;
    }
    else {
        if (driver->verbose)
            zclock_log ("I: (tcp) recv %zd bytes from %s",
                size, self->address);
        int rc = vtx_codec_bin_put (self->input, buffer, size);
        assert (rc == 0);

        Bool first_part = !self->more;
        Bool more;
        zmq_msg_t msg;
        zmq_msg_init (&msg);
        rc = vtx_codec_msg_get (self->input, &msg, &more);
        while (rc == 0) {
            if (vocket->routing == VTX_ROUTING_REQUEST) {
                //  TODO state check
                //  TODO Is reply expected to come from thes peering?
                int flags = ZMQ_DONTWAIT;
                if (more)
                    flags |= ZMQ_SNDMORE;
                zmq_sendmsg (vocket->msgpipe, &msg, flags);
            }
            else
            if (vocket->routing == VTX_ROUTING_REPLY) {
                //  TODO state check
                vocket->current_peering = self;
                int flags = ZMQ_DONTWAIT;
                if (more)
                    flags |= ZMQ_SNDMORE;
                zmq_sendmsg (vocket->msgpipe, &msg, flags);
            }
            else
            if (vocket->routing == VTX_ROUTING_ROUTER) {
                //  Send peering's ID
                if (first_part) {
                    size_t id_size = strlen(driver->scheme)
                                   + strlen("://")
                                   + strlen(self->address);
                    zmq_msg_t msg;
                    rc = zmq_msg_init_size (&msg, id_size + 1);
                    assert (rc == 0);
                    strcpy (zmq_msg_data (&msg), driver->scheme);
                    strcat (zmq_msg_data (&msg), "://");
                    strcat (zmq_msg_data (&msg), self->address);
                    zmq_sendmsg (vocket->msgpipe, &msg, ZMQ_DONTWAIT|ZMQ_SNDMORE);
                    zmq_msg_close (&msg);
                }
                int flags = ZMQ_DONTWAIT;
                if (more)
                    flags |= ZMQ_SNDMORE;
                zmq_sendmsg (vocket->msgpipe, &msg, flags);
            }
            else
            if (vocket->nomnom) {
                int flags = ZMQ_DONTWAIT;
                if (more)
                    flags |= ZMQ_SNDMORE;
                zmq_sendmsg (vocket->msgpipe, &msg, flags);
            }
            zmq_msg_close (&msg);
            zmq_msg_init (&msg);
            self->more = more;
            first_part = !more;
            rc = vtx_codec_msg_get (self->input, &msg, &more);
        }
#if 0
        else
            zclock_log ("W: unexpected message from %s - dropping", address);
        char *colon = strchr (address, ':');
        assert (colon);
        *colon = 0;
        strcpy (vocket->sender, address);
#endif
    }
    return size;
}


//  Converts a sockaddr_in to a string, returns static result

static char *
s_sin_addr_to_str (struct sockaddr_in *addr)
{
    static char
        address [24];
    snprintf (address, 24, "%s:%d",
        inet_ntoa (addr->sin_addr), ntohs (addr->sin_port));
    return address;
}

//  Converts a hostname:port into a sockaddr_in, returns static result
//  Asserts on badly formatted address.

static int
s_str_to_sin_addr (struct sockaddr_in *addr, char *address)
{
    int rc = 0;
    memset (addr, 0, IN_ADDR_SIZE);

    //  Take copy of address, then split into hostname and port
    char *hostname = strdup (address);
    char *port = strchr (hostname, ':');
    assert (port);
    *port++ = 0;

    addr->sin_family = AF_INET;
    addr->sin_port = htons (atoi (port));

    if (!inet_aton (hostname, &addr->sin_addr)) {
        struct hostent *phe = gethostbyname (hostname);
        if (phe)
            memcpy (&addr->sin_addr, phe->h_addr, phe->h_length);
        else {
            errno = EINVAL;
            rc = -1;
        }
    }
    free (hostname);
    return rc;
}

//  Set non-blocking mode on socket

static void
s_set_nonblock (int handle)
{
#   ifdef __WINDOWS__
    u_long noblock = 1;
    ioctlsocket (handle, FIONBIO, &noblock);
#   else
    fcntl (handle, F_SETFL, O_NONBLOCK | fcntl (handle, F_GETFL, 0));
#   endif
}

//  Close handle, remove poller from reactor

static void
s_close_handle (int handle, driver_t *driver)
{
    if (handle > 0) {
        zmq_pollitem_t item = { 0, handle };
        zloop_poller_end (driver->loop, &item);
        close (handle);
    }
}

//  Handle error from I/O operation, return 0 if the caller should
//  retry, -1 to abandon the operation.

static int
s_handle_io_error (char *reason)
{
#   ifdef __WINDOWS__
    switch (WSAGetLastError ()) {
        case WSAEINTR:        errno = EINTR;      break;
        case WSAEBADF:        errno = EBADF;      break;
        case WSAEWOULDBLOCK:  errno = EAGAIN;     break;
        case WSAEINPROGRESS:  errno = EAGAIN;     break;
        case WSAENETDOWN:     errno = ENETDOWN;   break;
        case WSAECONNRESET:   errno = ECONNRESET; break;
        case WSAECONNABORTED: errno = EPIPE;      break;
        case WSAESHUTDOWN:    errno = ECONNRESET; break;
        case WSAEINVAL:       errno = EPIPE;      break;
        default:              errno = GetLastError ();
    }
#   endif
    if (errno == EAGAIN
    ||  errno == ENETDOWN
    ||  errno == EPROTO
    ||  errno == ENOPROTOOPT
    ||  errno == EHOSTDOWN
    ||  errno == ENONET
    ||  errno == EHOSTUNREACH
    ||  errno == EOPNOTSUPP
    ||  errno == ENETUNREACH
    ||  errno == EWOULDBLOCK
    ||  errno == EINTR)
        return 0;           //  Ignore error and try again
    else
    if (errno == EPIPE
    ||  errno == ECONNRESET) {
        //  TODO: This message should not be left, it's for debugging
        zclock_log ("W: (tcp) error '%s' on %s", strerror (errno), reason);
        return -1;          //  Peer closed socket, abandon
    }
    else {
        zclock_log ("W: (tcp) error '%s' on %s", strerror (errno), reason);
        return -1;          //  Unexpected error, abandon
    }
}
