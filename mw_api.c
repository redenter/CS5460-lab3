#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "mpi.h"
#include "mw_api.h"
#include "queue.h"
#include <math.h>

#define MAX_MESSAGE_SIZE_IN_BYTE 500000
#define TIMEOUT 2 //in seconds
#define FAIL_RATE 0.1
/* run master-worker */
void MW_Run (int argc, char **argv, struct mw_api_spec *f){

  int rank, size;
  MPI_Comm mw_comm;
  srand(time(NULL)*getpid());

  MPI_Comm_rank( MPI_COMM_WORLD, &rank );
  if (rank == 0) 
    master( MPI_COMM_WORLD, argc, argv, f);
  else
    slave( MPI_COMM_WORLD, f);
}


int random_fail(double p)
{
  double rd = (double) rand() / (double)RAND_MAX;
  if(rd <= p){
  return 1;
  }
  return 0;
}

int F_Send(void *buf, int count, MPI_Datatype datatype, int dest, int tag, MPI_Comm comm, double fp)
{
  if (random_fail(fp)) {      
    printf("a processor fails\n");
    MPI_Finalize();
    exit (0);
    return 0;
  } else {
    return MPI_Send (buf, count, datatype, dest, tag, comm);
  }
}

int F_Isend(void *buf, int count, MPI_Datatype datatype, int dest, int tag, MPI_Comm comm, MPI_Request* request, double fp)
{
  if (random_fail(fp)) {      
    printf("a processor fails\n");
    MPI_Finalize();
    exit (0);
    return 0;
    
  } else{
    return MPI_Isend (buf, count, datatype, dest, tag, comm, request);
  }
}

char * serialize_works(int nWorks, mw_work_t ** works, int work_sz){
  char * b = malloc(work_sz*nWorks);
  for(int i =0;i<nWorks;i++){
    memcpy(b +i*work_sz, *(works+i), work_sz);
  }
  return b;
}

mw_work_t * deserialize_work(char * buff, int work_sz){
  mw_work_t * works = malloc(work_sz);
  memcpy(works, buff, work_sz);
  return works;
}

int send_message(char * buf, int size,int dest, int tag, MPI_Comm comm){
  int n = size / MAX_MESSAGE_SIZE_IN_BYTE;
  for(int i =0;i<n;i++){
    MPI_Send(buf+(MAX_MESSAGE_SIZE_IN_BYTE*i), MAX_MESSAGE_SIZE_IN_BYTE,MPI_BYTE,dest,tag,comm);
  }

  if(MAX_MESSAGE_SIZE_IN_BYTE*n != size)
    MPI_Send(buf+MAX_MESSAGE_SIZE_IN_BYTE*n, size -MAX_MESSAGE_SIZE_IN_BYTE*n,MPI_BYTE,dest,tag,comm);    
}

int receive_message(char * buf, int size, int source, int tag, MPI_Comm comm){
  MPI_Status status;
  int n = size / MAX_MESSAGE_SIZE_IN_BYTE;
  for(int i =0;i<n;i++){
    MPI_Recv(buf+(MAX_MESSAGE_SIZE_IN_BYTE*i), MAX_MESSAGE_SIZE_IN_BYTE,MPI_BYTE,source,tag,comm,&status);
  }

  if(MAX_MESSAGE_SIZE_IN_BYTE*n != size)
    MPI_Recv(buf+MAX_MESSAGE_SIZE_IN_BYTE*n, size-MAX_MESSAGE_SIZE_IN_BYTE*n,MPI_BYTE,source,tag,comm,&status);
}

worker_t *next_due_worker(worker_t * workers[], int n){
  double min_time = INFINITY;
  worker_t * due_worker=NULL;
  for(int i=0;i<n;i++){
    if(workers[i]->idle == 1 || workers[i]->alive == 0)
      continue;
    if(workers[i]->last_work_received_at < min_time){
      due_worker = workers[i];
      min_time = workers[i]->last_work_received_at;
    }
  }
  return due_worker;
}

worker_t * find_worker_by_rank(int rank, worker_t * workers[]){
  return workers[rank-1];
}


int master(MPI_Comm global_comm, int argc, char** argv, struct mw_api_spec *f )
{ 
  int size;
  printf("Hello, I am a master\n");

  MPI_Comm_size(global_comm, &size );  
  mw_work_t ** works = f->create(argc, argv);

  //adding all work into the queue
  int totalWorks=0;
  while(*(works+totalWorks) != NULL){
    int count = 1;
    char * b = serialize_works(count, works + totalWorks, f->work_sz);
    totalWorks+=count;

    work_allocation_t* dat = (work_allocation_t *)malloc(sizeof(work_allocation_t ));
    dat->work_data = b;
    dat->size = count;
    Enqueue(dat);
  }

  mw_result_t * mw_results = malloc(f->res_sz*totalWorks);
  
  int total_workers = size-1;
  int total_free_workers = total_workers,total_alive_workers = total_workers;
  worker_t *workers[total_workers];
  for(int i=1;i<size;i++){
    worker_t *worker = malloc(sizeof(worker_t));
    worker->rank=i;
    worker->idle=1;
    worker->alive=1;
    workers[i-1] = worker;
  }

  MPI_Status status;
  int work_recvd = 0;


  while((queue_size()!=0 || total_free_workers != total_alive_workers) && total_alive_workers >0){
    for(int i=0;i<total_workers;i++){
      if(workers[i]->idle ==1 && workers[i]->alive ==1){
        //send work
        if(Front() == NULL)
          break;

        work_allocation_t * ser_work = Front();
        MPI_Send(ser_work->work_data, f->work_sz,MPI_BYTE,workers[i]->rank,0,global_comm);
        total_free_workers--;
        workers[i]->current_work_allocation = ser_work;
        workers[i]->idle =0;
        workers[i]->last_work_received_at = MPI_Wtime();
        printf("assign work to worker %d\n",workers[i]->rank);
        Dequeue();
      }
    }

    char result_buf[f->res_sz];
    MPI_Request request;
    int received = 1;

    worker_t * due_worker = next_due_worker(workers,total_workers);
    if(due_worker == NULL){
      continue;
    }
    printf("due worker's rank is %d\n",due_worker->rank);
    while(MPI_Wtime()<due_worker->last_work_received_at+TIMEOUT && total_free_workers < total_alive_workers){
      if(received==1){
        MPI_Irecv(result_buf,f->res_sz,MPI_BYTE,MPI_ANY_SOURCE,0,global_comm,&request);
      }

      MPI_Test(&request,&received,&status);
        if(received ==1){
          worker_t * sender = find_worker_by_rank(status.MPI_SOURCE, workers);
          if(sender->alive ==1){
            memcpy(((char *)mw_results) + work_recvd*f->res_sz, result_buf,f->res_sz);
            work_recvd++;
            sender->idle =1;
            total_free_workers++;
            printf("receive result from alive worker %d\n",status.MPI_SOURCE);
          }
          else{
            printf("receive result from dead worker %d\n",status.MPI_SOURCE );
          }
        }
    }
    if(received != 1)
      MPI_Cancel(&request);

    if(due_worker->idle == 0){
      printf("due worker times out\n");
      due_worker->alive =0;
      total_alive_workers--;
      Enqueue(due_worker->current_work_allocation);
    }

    if(total_alive_workers != 0){
      printf("-----after waiting for results\n");
      printf("total free:%d\n", total_free_workers);    
      printf("total alive:%d\n", total_alive_workers);
      printf("queue size is %d\n",queue_size());
    }
  }

  if(total_alive_workers == 0 && queue_size() != 0)
    printf("total failure!!!\n");
  else
    f->result(totalWorks,mw_results);

  free(works);
  free(mw_results);
  MPI_Abort(MPI_COMM_WORLD,0);
  
  return 0;  

}

/*master failures
  1. choosing a proxy master every time the master boots up or is chosen
  2. the proxy master needs all the progress info, atleast of the works that have been completed. 
  Either the master could send them or the workers send that info. 


  //terminate command?



*/

int slave(MPI_Comm global_comm, struct mw_api_spec *f)
{
    int  rank;
    MPI_Status status;
    int nWorks=1;

    MPI_Comm_rank(global_comm, &rank);
    
    while(1){
      char * buf = malloc(f->work_sz*nWorks);
      MPI_Recv(buf,f->work_sz*nWorks,MPI_BYTE,0,0,global_comm,&status);
      //// execute works

      char result_buf[f->res_sz];    
      for(int i = 0;i<nWorks;i++){
        mw_work_t * work = deserialize_work(buf+i*(f->work_sz),f->work_sz);
        mw_result_t * result = f->compute(work);
        memcpy(result_buf, result,f->res_sz);
        F_Send(result_buf,f->res_sz,MPI_BYTE,0,0,global_comm,FAIL_RATE);
        printf("worker %d sends successfully\n",rank);
        free(work);
      }
      free(buf);
    }
    return 0;
}
