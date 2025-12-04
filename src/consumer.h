/* SPDX-License-Identifier: BSD-3-Clause */

#ifndef __SO_CONSUMER_H__
#define __SO_CONSUMER_H__

#include <pthread.h>

#include "ring_buffer.h"
#include "packet.h"

typedef struct so_consumer_ctx_t {
	struct so_ring_buffer_t *producer_rb;

	pthread_mutex_t *dequeue_mutex; // ordonare dequeue si incrementare seq_counter
	size_t *seq_counter;

	pthread_mutex_t *log_mutex; // protejeaza scrierea in fisier
	pthread_cond_t *log_cond; // coordoneaza ordinea de scriere in functie de secventa
	size_t *next_seq_to_write; // urmatoarea secventa care are voie sa scrie

	int *out_fd; // descriptorul fisierului de iesire
} so_consumer_ctx_t;

int create_consumers(pthread_t *tids,
		     int num_consumers,
		     so_ring_buffer_t *rb,
		     const char *out_filename);

#endif /* __SO_CONSUMER_H__ */
