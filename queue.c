#include<stdio.h>
#include<stdlib.h>
#include "mw_api.h"
#include "queue.h"

// To Enqueue an integer
void Enqueue( queue *q,work_allocation_t *d) {
	struct Node* temp = 
		(struct Node*)malloc(sizeof(struct Node));
	temp->data =d; 
	temp->next = NULL;
	if(q->front == NULL && q->rear == NULL){
		q->front = q->rear = temp;
		return;
	}
	q->rear->next = temp;
	q->rear = temp;
}

// To Dequeue an integer.
void Dequeue( queue *q) {
	struct Node* temp = q->front;
	if(q->front == NULL) {
		printf("Queue is Empty\n");
		return;
	}
	if(q->front == q->rear) {
		q->front = q->rear = NULL;
	}
	else {
		q->front = q->front->next;
	}
	free(temp);
}

work_allocation_t * Front( queue *q) {
	if(q->front == NULL) {
		printf("Queue is empty\n");
		return NULL;
	}
	//     printf("before mpi!! %d\n",(front->data->size));

	return q->front->data;
}

int queue_size( queue *q){
	struct Node* temp = q->front;
	
	int count = 0;
	while(temp!=NULL){
		temp = temp->next;
		count ++;
	}
	return count;
}

// void create_queue(struct queue *q){
// 	q->Front = Front;
// 	q->queue_size = queue_size;
// 	q->Enqueue = Enqueue;
// 	q->Dequeue = Dequeue;
// }

// void Print() {
// 	struct Node* temp = front;
// 	while(temp != NULL) {
// 		printf("%d ",temp->data);
// 		temp = temp->next;
// 	}
// 	printf("\n");
// }

// int main(){
// 	/* Drive code to test the implementation. */
// 	// Printing elements in Queue after each Enqueue or Dequeue 
// 	work_allocation_t *dat = malloc(sizeof(work_allocation_t));
// 	dat->size = 2;
// 	char* d = malloc(sizeof(char)*2);
// 	*(d+0) = '1';
// 	*(d+1) = '2';
// 	dat->data = d;

// 	Enqueue(dat); //Print(); 
// 	printf("size of the array is %d\n",size());
// 	Enqueue(dat); //Print();
// 		printf("size of the array is %d\n",size());
// 	Enqueue(dat); //Print();
// 		printf("size of the array is %d\n",size());
// 	Dequeue();  //Print();
// 		printf("size of the array is %d\n",size());
// 	Enqueue(dat); //Print();
// 	printf("size of the array is %d\n",size());
// }