


struct Node {
	work_allocation_t *data;
	struct Node* next;
};

typedef struct queue_s{
	struct Node *front, *rear;
}queue;

	work_allocation_t * Front( queue *q);
	void Dequeue( queue *q);
	void Enqueue(queue *q,work_allocation_t *d);
	int queue_size( queue *q);
