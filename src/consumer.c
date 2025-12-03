// SPDX-License-Identifier: BSD-3-Clause

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>

#include "consumer.h"
#include "ring_buffer.h"
#include "packet.h"
#include "utils.h"

void consumer_thread(so_consumer_ctx_t *ctx)
{
	char pkt_buf[PKT_SZ];
	char line_buf[128];

	while (1) {
		ssize_t rc;
		size_t my_ticket = 0;

		pthread_mutex_lock(ctx->dequeue_mutex);

		rc = ring_buffer_dequeue(ctx->producer_rb, pkt_buf, PKT_SZ);
		if (rc <= 0) {
			pthread_mutex_unlock(ctx->dequeue_mutex);
			break;
		}

		my_ticket = *ctx->ticket_counter;
		(*ctx->ticket_counter)++;

		pthread_mutex_unlock(ctx->dequeue_mutex);

		struct so_packet_t *pkt = (struct so_packet_t *)pkt_buf;
		unsigned long h = packet_hash(pkt);
		so_action_t action = process_packet(pkt);

		const char *status;
		int n;

		if (action == PASS)
			status = "PASS";
		else
			status = "DROP";

		n = snprintf(line_buf, sizeof(line_buf),
			     "%s %016lx %lu\n",
			     status, h, pkt->hdr.timestamp);

		pthread_mutex_lock(ctx->log_mutex);

		while (my_ticket != *ctx->next_ticket_to_write)
			pthread_cond_wait(ctx->log_cond, ctx->log_mutex);

		write(*ctx->out_fd, line_buf, (size_t)n);

		(*ctx->next_ticket_to_write)++;
		pthread_cond_broadcast(ctx->log_cond);

		pthread_mutex_unlock(ctx->log_mutex);
	}

	free(ctx);
}

static void *consumer_start(void *arg)
{
	consumer_thread((so_consumer_ctx_t *)arg);
	return NULL;
}

int create_consumers(pthread_t *tids,
		     int num_consumers,
		     struct so_ring_buffer_t *rb,
		     const char *out_filename)
{
	int *out_fd;
	pthread_mutex_t *dq_mutex;
	pthread_mutex_t *log_mtx;
	pthread_cond_t *log_condv;
	size_t *counter;
	size_t *next_ticket;

	out_fd = malloc(sizeof(int));
	if (out_fd == NULL) {
		perror("malloc out_fd");
		exit(EXIT_FAILURE);
	}

	*out_fd = open(out_filename, O_WRONLY | O_CREAT | O_TRUNC, 0666);
	if (*out_fd < 0) {
		perror("open output");
		free(out_fd);
		exit(EXIT_FAILURE);
	}

	dq_mutex = malloc(sizeof(pthread_mutex_t));
	log_mtx = malloc(sizeof(pthread_mutex_t));
	log_condv = malloc(sizeof(pthread_cond_t));
	counter = malloc(sizeof(size_t));
	next_ticket = malloc(sizeof(size_t));

	if (!dq_mutex || !log_mtx || !log_condv || !counter || !next_ticket) {
		perror("malloc sync vars");
		free(out_fd);
		free(dq_mutex);
		free(log_mtx);
		free(log_condv);
		free(counter);
		free(next_ticket);
		exit(EXIT_FAILURE);
	}

	pthread_mutex_init(dq_mutex, NULL);
	pthread_mutex_init(log_mtx, NULL);
	pthread_cond_init(log_condv, NULL);

	*counter = 0;
	*next_ticket = 0;

	for (int i = 0; i < num_consumers; i++) {
		so_consumer_ctx_t *ctx = malloc(sizeof(*ctx));

		if (ctx == NULL) {
			perror("malloc ctx");
			exit(EXIT_FAILURE);
		}

		ctx->producer_rb = rb;
		ctx->dequeue_mutex = dq_mutex;
		ctx->ticket_counter = counter;
		ctx->log_mutex = log_mtx;
		ctx->log_cond = log_condv;
		ctx->next_ticket_to_write = next_ticket;
		ctx->out_fd = out_fd;

		if (pthread_create(&tids[i], NULL,
				   consumer_start,
				   ctx) != 0) {
			perror("pthread_create");
			exit(EXIT_FAILURE);
		}
	}

	return num_consumers;
}
