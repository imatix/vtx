/*  =====================================================================
    vtx_decoder - message decoder

    Decoder decodes 0MQ messages delivered in binary form. It's
    implemented as a FSM with two states.

    ---------------------------------------------------------------------
    Copyright (c) 1991-2011 iMatix Corporation <www.imatix.com>
    Copyright other contributors as noted in the AUTHORS file.

    This file is part of VTX, the 0MQ virtual transport interface:
    http://vtx.zeromq.org.

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

#ifndef __VTX_DECODER_INCLUDED__
#define __VTX_DECODER_INCLUDED__

#include "czmq.h"

typedef struct _vtx_decoder vtx_decoder_t;
typedef struct _vtx_decoder_message message_t;
typedef size_t (*state_func_t)(vtx_decoder_t *, byte *, size_t);

// This structure ties 0MQ message with its more flag
struct _vtx_decoder_message {
    zmq_msg_t msg;
    Bool more;
};

// Decoder representation
struct _vtx_decoder {
    message_t *queue;           //  Queue holding decoded messages
    size_t queue_size;          //  Size of message queue
    size_t queue_head;          //  Least recently decoded message
    size_t queue_tail;          //  Most recently decoded message
    size_t header_bytes_read;   //  Number of bytes read from header
    size_t payload_bytes_read;  //  Number of bytes read from payload
    state_func_t state_func;    //  Decode function for current state
    byte header [10];           //  Buffer used for header decoding
};

//  Is message queue full?
#define QUEUE_FULL \
    ((self->queue_tail + 1) % self->queue_size == self->queue_head)

//  Is message queue empty?
#define QUEUE_EMPTY \
    (self->queue_head == self->queue_tail)

//  Create new decoder
vtx_decoder_t *
    vtx_decoder_new (size_t queue_size);

//  Destroy decoder and release all resources it holds
void
    vtx_decoder_destroy (vtx_decoder_t **self_p);

//  Reset decoder
void
    vtx_decoder_reset (vtx_decoder_t *self);

//  Decode another chunk of data
int
    vtx_decoder_bin_put (vtx_decoder_t *self, byte *data, size_t size);

//  Fetch decoded message
int
    vtx_decoder_msg_get (vtx_decoder_t *self, zmq_msg_t *msg, Bool *more_p);

//  Decode message header
static size_t
    s_vtx_read_header (vtx_decoder_t *self, byte *data, size_t size);

//  Decode message payload
static size_t
    s_vtx_read_payload (vtx_decoder_t *self, byte *data, size_t size);

static int
    s_random (int limit);

//  -------------------------------------------------------------------------
//  Create new decoder instance. Once created, the decoder is ready
//  to parse binary data.

vtx_decoder_t *
vtx_decoder_new (size_t queue_size)
{
    vtx_decoder_t *self = (vtx_decoder_t *) zmalloc (sizeof (vtx_decoder_t));
    assert (self);
    self->queue = (message_t *) malloc (sizeof (message_t) * (queue_size + 1));
    assert (self->queue);
    self->queue_size = queue_size + 1;  // one slot is always left unused
    self->queue_head = 0;
    self->queue_tail = 0;
    self->header_bytes_read = 0;
    self->payload_bytes_read = 0;
    self->state_func = s_vtx_read_header;

    return self;
}

//  -------------------------------------------------------------------------
//  Destroy decoder and all messages it holds

void
vtx_decoder_destroy (vtx_decoder_t **self_p)
{
    assert (self_p);
    if (*self_p) {
        vtx_decoder_t *self = *self_p;
        while (!QUEUE_EMPTY) {
            zmq_msg_t *msg = &self->queue [self->queue_head].msg;
            self->queue_head = (self->queue_head + 1) % self->queue_size;
            zmq_msg_close (msg);
        }
        free (self->queue);
        free (self);
        *self_p = NULL;
    }
}

//  -------------------------------------------------------------------------
//  Reset decoder. This makes decoder to forget all its history.

void
vtx_decoder_reset (vtx_decoder_t *self)
{
    while (!QUEUE_EMPTY) {
        zmq_msg_t *msg = &self->queue [self->queue_head].msg;
        self->queue_head = (self->queue_head + 1) % self->queue_size;
        zmq_msg_close (msg);
    }
    self->header_bytes_read = 0;
    self->payload_bytes_read = 0;
    self->state_func = s_vtx_read_header;
}

//  -------------------------------------------------------------------------
//  Feed serialized data into the decoder. Returns 0 if all bytes were
//  accepted; -1 otherwise.

int
vtx_decoder_bin_put (vtx_decoder_t *self, byte *data, size_t size)
{
    while (size > 0) {
        if (QUEUE_FULL)
            return -1;
        size_t bytes_read = self->state_func (self, data, size);
        data += bytes_read;
        size -= bytes_read;
    }
    return 0;
}

//  -------------------------------------------------------------------------
//  Get the next message. Messages are returned in FIFO order

int
vtx_decoder_msg_get (vtx_decoder_t *self, zmq_msg_t *msg, Bool *more_p)
{
    if (QUEUE_EMPTY)
        return -1;
    zmq_msg_copy (msg, &self->queue [self->queue_head].msg);
    *more_p = self->queue [self->queue_head].more;
    self->queue_head = (self->queue_head + 1) % self->queue_size;
    return 0;
}

//  -------------------------------------------------------------------------
//  Decode message header. Returns the number of bytes consumed.

static size_t
s_vtx_read_header (vtx_decoder_t *self, byte *data, size_t size)
{
    assert (!QUEUE_FULL);
    size_t bytes_read = 0;
    if (self->header_bytes_read < 2) {
        size_t n = 2 - self->header_bytes_read;
        if (n > size)
            n = size;
        memcpy (&self->header [self->header_bytes_read], data, n);
        self->header_bytes_read += n;
        data += n;
        size -= n;
        bytes_read += n;
    }
    if (self->header [0] == 0xff && self->header_bytes_read < 10) {
        size_t n = 10 - self->header_bytes_read;
        if (n > size)
            n = size;
        memcpy (&self->header [self->header_bytes_read], data, n);
        self->header_bytes_read += n;
        bytes_read += n;
    }

    if (self->header_bytes_read > 1) {
        if (self->header [0] < 0xff
        ||  self->header_bytes_read == 10) {
            uint64_t frame_size;
            Bool more;
            if (self->header [0] < 0xff) {
                frame_size = (uint64_t) (self->header [0]);
                more = (self->header [1] == 1);
            }
            else {
                frame_size = ((uint64_t) (self->header [1]) << 56)
                           + ((uint64_t) (self->header [2]) << 48)
                           + ((uint64_t) (self->header [3]) << 40)
                           + ((uint64_t) (self->header [4]) << 32)
                           + ((uint64_t) (self->header [5]) << 24)
                           + ((uint64_t) (self->header [6]) << 16)
                           + ((uint64_t) (self->header [7]) << 8)
                           + ((uint64_t) (self->header [8]));
                more = (self->header [9] == 1);
            }
            assert (frame_size > 0);
            zmq_msg_t *msg = &self->queue [self->queue_tail].msg;
            zmq_msg_init_size (msg, frame_size - 1);
            self->queue [self->queue_tail].more = more;
            //  No payload; get ready to decode next message
            if (frame_size == 1) {
                self->queue_tail = (self->queue_tail + 1) % self->queue_size;
                self->header_bytes_read = 0;
            }
            //  Get ready to decode message payload
            else
                self->state_func = s_vtx_read_payload;
        }
    }
    return bytes_read;
}

//  -------------------------------------------------------------------------
//  Decode message payload.

static size_t
s_vtx_read_payload (vtx_decoder_t *self, byte *data, size_t size)
{
    zmq_msg_t *msg = &self->queue [self->queue_tail].msg;
    size_t n = zmq_msg_size (msg) - self->payload_bytes_read;
    //  Copy and wait for more data
    if (n > size) {
        n = size;
        memcpy (zmq_msg_data (msg) + self->payload_bytes_read, data, n);
        self->payload_bytes_read += n;
    }
    //  Copy and get ready to decode next message
    else {
        memcpy (zmq_msg_data (msg) + self->payload_bytes_read, data, n);
        self->queue_tail = (self->queue_tail + 1) % self->queue_size;
        self->header_bytes_read = 0;
        self->payload_bytes_read = 0;
        self->state_func = s_vtx_read_header;
    }
    return n;
}

struct vtx_decoder_test_msg {
    size_t length;  //  message length
    Bool more;      //  more flag
    byte *data;     //  message data
};

//  Unit test
static void
vtx_decoder_selftest (void)
{
    struct vtx_decoder_test_msg msgs [1024];
    byte buf [1024 * (1024 + 10)];
    size_t ptr = 0;
    int i, j;

    //  Phase 1: generate test messages
    for (i = 0; i < 1024; i++) {
        size_t length = s_random (1024);
        msgs [i].length = length;
        msgs [i].more = s_random (2)? TRUE: FALSE;
        msgs [i].data = malloc (length);
        int j;
        for (j = 0; j < length; j++)
            msgs [i].data [j] = s_random (255);
    }

    //  Phase 2: serialize test messages
    for (i = 0; i < 1024; i++) {
        size_t frame_size = msgs [i].length + 1;
        if (frame_size < 255)
            buf [ptr++] = (byte) frame_size;
        else {
            buf [ptr++] = 0xff;
            buf [ptr++] = (byte) (frame_size >> 56);
            buf [ptr++] = (byte) (frame_size >> 48);
            buf [ptr++] = (byte) (frame_size >> 40);
            buf [ptr++] = (byte) (frame_size >> 32);
            buf [ptr++] = (byte) (frame_size >> 24);
            buf [ptr++] = (byte) (frame_size >> 16);
            buf [ptr++] = (byte) (frame_size >>  8);
            buf [ptr++] = (byte) (frame_size      );
        }
        buf [ptr++] = msgs [i].more? 1 : 0;
        memcpy (&buf [ptr], msgs [i].data, msgs [i].length);
        ptr += msgs [i].length;
    }

    //  Phase 3: feed data to decoder in random size chunks
    vtx_decoder_t *decoder = vtx_decoder_new (1024);
    assert (decoder);
    size_t bytes_decoded = 0;
    while (bytes_decoded < ptr) {
        size_t n = s_random (1024);
        if (n > ptr - bytes_decoded)
            n = ptr - bytes_decoded;
        int rc = vtx_decoder_bin_put (decoder, &buf [bytes_decoded], n);
        assert (rc == 0);
        bytes_decoded += n;
    }

    //  Phase 4: fetch decoded messages and check them
    for (i = 0; i < 1024; i++) {
        zmq_msg_t msg;
        Bool more;
        zmq_msg_init (&msg);
        int rc = vtx_decoder_msg_get (decoder, &msg, &more);
        assert (rc == 0);
        assert (zmq_msg_size (&msg) == msgs [i].length);
        assert (more == msgs [i].more);
        assert (memcmp (zmq_msg_data (&msg), msgs [i].data, msgs [i].length) == 0);
        zmq_msg_close (&msg);
    }
}

int main () {
    vtx_decoder_selftest ();
    return 0;
}

//  Fast pseudo-random number generator

static int
s_random (int limit)
{
    static uint32_t value = 0;
    if (value == 0)
        value = (uint32_t) (time (NULL));

    value = (value ^ 61) ^ (value >> 16);
    value = value + (value << 3);
    value = value ^ (value >> 4);
    value = value * 0x27d4eb2d;
    value = value ^ (value >> 15);
    return value % limit;
}

#endif
