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
	char pkt_buf[PKT_SZ]; // buffer local pentru un pachet citit din ring buffer
	char line_buf[128];	// buffer pentru linia generata din pachet

	// bucla pentru thread-ul consumer
	while (1) {
		ssize_t rc;	// cod de retur de la dequeue
		size_t my_seq = 0; // secventa asociata acestui pachet

		pthread_mutex_lock(ctx->dequeue_mutex);	// blocam zona dequeue + alocare de secventa

		// se citeste un pachet din ring buffer
		rc = ring_buffer_dequeue(ctx->producer_rb, pkt_buf, PKT_SZ);
		if (rc <= 0) {
			pthread_mutex_unlock(ctx->dequeue_mutex); // eliberare mutex daca nu s-a putut citi
			break;
		}

		my_seq = *ctx->seq_counter; // secventa curenta din acest pachet
		(*ctx->seq_counter)++; // se pregateste secventa pentru urmatorul pachet

		pthread_mutex_unlock(ctx->dequeue_mutex); // eliberare mutex pentru un alt consumer daca intra

		struct so_packet_t *pkt = (struct so_packet_t *)pkt_buf; // interpretam buffer-ul ca pachet
		unsigned long h = packet_hash(pkt);	// hash pachet
		so_action_t action = process_packet(pkt);

		const char *status;
		int n;

		// daca pachetul este valid/acceptat
		if (action == PASS)
			status = "PASS";
		else
			status = "DROP";

		// linia pentru acest pachet
		n = snprintf(line_buf, sizeof(line_buf),
			     "%s %016lx %lu\n",
			     status, h, pkt->hdr.timestamp);

		pthread_mutex_lock(ctx->log_mutex);	// protejare scriere si acces la next_seq_to_write

		// astept randul secventei mele
		while (my_seq != *ctx->next_seq_to_write)
			pthread_cond_wait(ctx->log_cond, ctx->log_mutex);

		write(*ctx->out_fd, line_buf, (size_t)n); // scriere linie in fisier

		(*ctx->next_seq_to_write)++; // permit urmatoarei secvente sa scrie
		pthread_cond_broadcast(ctx->log_cond);

		pthread_mutex_unlock(ctx->log_mutex);
	}

	free(ctx);
}

/*
 * wrapper pentru pornirea unui thread consumer care
 * converteste argumentul generic void* in contextul specific
 * si apeleaza consumer_thread(), apoi se intoarce cu NULL
 */
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
	size_t *next_seq;

	out_fd = malloc(sizeof(int)); // aloc memorie pentru descriptorul de fisier
	if (out_fd == NULL) {
		perror("malloc out_fd");
		exit(EXIT_FAILURE);
	}

	*out_fd = open(out_filename, O_WRONLY | O_CREAT | O_TRUNC, 0666); // deschid fisierul pentru scriere
	// se verifica daca open a reusit
	if (*out_fd < 0) {
		perror("open output");
		free(out_fd);
		exit(EXIT_FAILURE);
	}

	dq_mutex = malloc(sizeof(pthread_mutex_t)); // mutex pentru dequeue
	log_mtx = malloc(sizeof(pthread_mutex_t)); // mutex pentru log
	log_condv = malloc(sizeof(pthread_cond_t));	// variabila conditie
	counter = malloc(sizeof(size_t)); // counter-ul de secvente
	next_seq = malloc(sizeof(size_t)); // aloc next_seq_to_write

	if (!dq_mutex || !log_mtx || !log_condv || !counter || !next_seq) {
		perror("malloc sync vars");
		free(out_fd);
		free(dq_mutex);
		free(log_mtx);
		free(log_condv);
		free(counter);
		free(next_seq);
		exit(EXIT_FAILURE);
	}

	pthread_mutex_init(dq_mutex, NULL); // initializez mutex-ul pentru dequeue
	pthread_mutex_init(log_mtx, NULL); // mutex-ul pentru log
	pthread_cond_init(log_condv, NULL);	// initializam variabila de conditie pentru log

	*counter = 0; // prima secventa alocata va fi 0
	*next_seq = 0; // prima secventa permisa la scriere este 0

	for (int i = 0; i < num_consumers; i++) {
		so_consumer_ctx_t *ctx = malloc(sizeof(*ctx)); // context individual pentru fiecare thread

		if (ctx == NULL) {
			perror("malloc ctx");
			exit(EXIT_FAILURE);
		}

		ctx->producer_rb = rb; // setare ring buffer
		ctx->dequeue_mutex = dq_mutex; // mutex comun pentru dequeue si secvente
		ctx->seq_counter = counter; // pointer la generatorul global de secvente
		ctx->log_mutex = log_mtx; // mutex pentru log
		ctx->log_cond = log_condv; // variabila de conditie pentru ordonare
		ctx->next_seq_to_write = next_seq; // pointer la secventa curenta permisa la scriere
		ctx->out_fd = out_fd; // pointer la descriptorul de fisier comun

		// creare thread consumer
		if (pthread_create(&tids[i], NULL,
				   consumer_start,
				   ctx) != 0) {
			perror("pthread_create");
			exit(EXIT_FAILURE);
		}
	}

	return num_consumers; // numarul de thread-uri create
}
