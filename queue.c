#include<stdio.h>
#include<stdlib.h>
#include "mw_api.h"

// work_allocation_t{
//   size_t size;
//   char *data;
// };

struct Node {
	work_allocation_t *data;
	struct Node* next;
};

// Two glboal variables to store address of front and rear nodes. 
struct Node* front = NULL;
struct Node* rear = NULL;

// To Enqueue an integer
void Enqueue(work_allocation_t *d) {
	struct Node* temp = 
		(struct Node*)malloc(sizeof(struct Node));
	temp->data =d; 
	temp->next = NULL;
	if(front == NULL && rear == NULL){
		front = rear = temp;
		return;
	}
	rear->next = temp;
	rear = temp;
}

// To Dequeue an integer.
void Dequeue() {
	struct Node* temp = front;
	if(front == NULL) {
		printf("Queue is Empty\n");
		return;
	}
	if(front == rear) {
		front = rear = NULL;
	}
	else {
		front = front->next;
	}
	free(temp);
}

work_allocation_t * Front() {
	if(front == NULL) {
		printf("Queue is empty\n");
		return NULL;
	}
	//     printf("before mpi!! %d\n",(front->data->size));

	return front->data;
}

int queue_size(){
	struct Node* temp = front;
	
	int count = 0;
	while(temp!=NULL){
		temp = temp->next;
		count ++;
	}
	return count;
}

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