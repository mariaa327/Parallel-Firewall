// SPDX-License-Identifier: BSD-3-Clause

#include "ring_buffer.h"
#include <stdlib.h>
#include <string.h>

int ring_buffer_init(so_ring_buffer_t *ring, size_t cap)
{
	int ok;

	// verificare parametrii de intrare
	if (!ring || cap == 0)
		return -1;

	ring->data = malloc(cap);
	if (!ring->data)
		return -1;

	ring->cap = cap; // capacitate totala
	ring->len = 0; // lungime 0 nu avem date in buffer
	ring->read_pos = 0; // prima pozitie de citire
	ring->write_pos = 0; // prima pozitie de scriere
	ring->stopped = 0; // ring buffer activ

	// initializare mutex pentru sincronizare
	ok = pthread_mutex_init(&ring->mutex, NULL);
	if (ok != 0) {
		free(ring->data);
		return -1;
	}

	// initializare variabila de conditie pentru not_full
	ok = pthread_cond_init(&ring->not_full, NULL);
	if (ok != 0) {
		pthread_mutex_destroy(&ring->mutex);
		free(ring->data);
		return -1;
	}

	// initializare variabila de conditie pentru not_empty
	ok = pthread_cond_init(&ring->not_empty, NULL);
	if (ok != 0) {
		pthread_cond_destroy(&ring->not_full);
		pthread_mutex_destroy(&ring->mutex);
		free(ring->data);
		return -1;
	}

	return 0;
}

ssize_t ring_buffer_enqueue(so_ring_buffer_t *ring, void *data, size_t size)
{
	size_t available_to_end; // numar de bytes disponibili pana la finalul buffer-ului

	if (!ring || !data || size == 0)
		return -1;

	pthread_mutex_lock(&ring->mutex); // mutex pentru acces la ring

	// cat timp nu e stop si nu avem loc suficient se asteapta semnal ca buffer ul nu e plin
	while (!ring->stopped && (ring->len + size > ring->cap))
		pthread_cond_wait(&ring->not_full, &ring->mutex);

	// daca s-a dat stop se elibereaza mutex si se intoarce eroare
	if (ring->stopped) {
		pthread_mutex_unlock(&ring->mutex);
		return -1;
	}

	available_to_end = ring->cap - ring->write_pos;
	if (available_to_end >= size) {
		memcpy(ring->data + ring->write_pos, data, size); // se copiaza in continuare
		ring->write_pos += size;
		// daca am ajuns la final se face wrap la pozitia 0
		if (ring->write_pos == ring->cap)
			ring->write_pos = 0;
	} else {
		memcpy(ring->data + ring->write_pos, data, available_to_end); // se scrie prima parte pana la final
		// se scrie restu de la inceputul buffer ului
		memcpy(ring->data, (char *)data + available_to_end, size - available_to_end);
		ring->write_pos = size - available_to_end;
	}

	ring->len += size;

	pthread_cond_signal(&ring->not_empty); // semnal ca buffer ul nu mai e gol
	pthread_mutex_unlock(&ring->mutex); // eliberare mutex

	return size;
}

ssize_t ring_buffer_dequeue(so_ring_buffer_t *ring, void *data, size_t size)
{
	size_t available_to_end;

	if (!ring || !data || size == 0)
		return -1;

	pthread_mutex_lock(&ring->mutex);

	while (!ring->stopped && ring->len < size)
		pthread_cond_wait(&ring->not_empty, &ring->mutex);

	if (ring->len < size && ring->stopped) {
		pthread_mutex_unlock(&ring->mutex);
		return -1;
	}

	available_to_end = ring->cap - ring->read_pos;

	if (available_to_end >= size) {
		memcpy(data, ring->data + ring->read_pos, size);
		ring->read_pos += size;
		if (ring->read_pos == ring->cap)
			ring->read_pos = 0;
	} else {
		memcpy(data, ring->data + ring->read_pos, available_to_end);
		memcpy((char *)data + available_to_end, ring->data, size - available_to_end);
		ring->read_pos = size - available_to_end;
	}

	ring->len -= size;

	pthread_cond_signal(&ring->not_full);
	pthread_mutex_unlock(&ring->mutex);

	return size;
}

void ring_buffer_destroy(so_ring_buffer_t *ring)
{
	// nu avem ce distruge
	if (!ring)
		return;

	free(ring->data); // eliberare buffer

	pthread_mutex_destroy(&ring->mutex); // distrugere mutex
	pthread_cond_destroy(&ring->not_full); // distrugere not_full
	pthread_cond_destroy(&ring->not_empty); // distrugere not_empty
}

void ring_buffer_stop(so_ring_buffer_t *ring)
{
	if (!ring)
		return;

	pthread_mutex_lock(&ring->mutex);
	ring->stopped = 1; // se marcheaza ca fiind oprit

	pthread_mutex_unlock(&ring->mutex);
}
