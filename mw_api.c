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
#define TAG_TERMINATE 404
/* run master-worker */
void MW_Run (int argc, char **argv, struct mw_api_spec *f){

  int rank, size;
  MPI_Comm mw_comm;
  srand(time(NULL)*getpid());


  MPI_Comm_rank( MPI_COMM_WORLD, &rank );
  if (rank == 0) 
    master( MPI_COMM_WORLD, argc, argv, f);
  else if(rank == 1)
    proxy_master(MPI_COMM_WORLD, argc, argv, f);
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
   int rank;
  MPI_Comm_rank(comm, &rank);

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

char * serialize_worker_status(worker_t * workers[], int n){

  int alive_info[n];
  int size = 0;

  int work_ids[n];

  for(int i=0;i<n;i++){
    alive_info[i] = workers[i]->alive;
    //printf("workers[i]->current_work_allocation%s\n",workers[i]->current_work_allocation);
    if(workers[i]->current_work_allocation == NULL)
      work_ids[i] = -1;
    else
      work_ids[i] = (workers[i]->current_work_allocation)->work_id;
  }

  char *b = malloc(sizeof(int)*(2*n));
  memcpy(b,work_ids,n*sizeof(int));
  memcpy(b + (n*sizeof(int)),alive_info,n*sizeof(int));
  return b;
}

void update_recvd_works(int *received_work_ids,int totalWorks,worker_t * workers[],int total_workers){
  for(int i=0;i<total_workers;i++){
    if(workers[i]->alive==1 && workers[i]->current_work_allocation != NULL){
      int id = workers[i]->current_work_allocation->work_id;
      if( id>0 && id<totalWorks)
        *(received_work_ids+id) = 1;
    }
  }
}

void update_worker_status(char *b,worker_t *workers[],int total_workers){
  int *alive_info = malloc(sizeof(int)*total_workers);
  int *work_ids = malloc(sizeof(int)*total_workers);
  

   memcpy(work_ids,b,total_workers*sizeof(int));
   memcpy(alive_info,b + (total_workers*sizeof(int)),total_workers*sizeof(int));

  // printf("should be good!!!!!!!\n");
  for(int i=0;i<total_workers;i++){
    if(alive_info[i]!=0 && alive_info[i]!=1)
        alive_info[i] = 1;
    workers[i]->alive = alive_info[i];
    workers[i]->idle = 1;
    (workers[i]->current_work_allocation)->work_id = work_ids[i];
  }
free(alive_info);
free(work_ids);


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
  return workers[rank-2];
}


int get_work_done(int rank,MPI_Comm global_comm,queue *q,worker_t * workers[], mw_result_t * mw_results ,int total_workers, int total_free_workers, int total_alive_workers, struct mw_api_spec *f, int totalWorks){
  
  int work_recvd = 0;
  int proxy_master_rank = 1;
  MPI_Status status;
  int kill;

  while((queue_size(q)!=0 || total_free_workers != total_alive_workers) && total_alive_workers >0){

    if(rank != proxy_master_rank){

       MPI_Iprobe(proxy_master_rank, TAG_TERMINATE, global_comm, &kill, &status);
        if(kill==1){
          printf("primary master dies!!!!!!!!!!!!!!!!!!\n");
          MPI_Finalize();
          exit(1);
          return 0;
        }

      //send results and worker status to proxy master
      char * b = serialize_worker_status(workers, total_workers);
      int b_size = sizeof(int)*(2*total_workers);
      char *all_data_b = malloc( b_size+ (f->res_sz)*work_recvd);
      memcpy(all_data_b,b,b_size);
      memcpy(all_data_b+b_size,mw_results,f->res_sz*work_recvd);

      //MPI_Send(mw_results,(f->res_sz)*work_recvd,MPI_BYTE,proxy_master_rank,0,global_comm);
      F_Send(all_data_b,b_size +(f->res_sz)*work_recvd ,MPI_BYTE,proxy_master_rank,0,global_comm,FAIL_RATE);
      //MPI_Send(all_data_b,b_size +(f->res_sz)*work_recvd ,MPI_BYTE,proxy_master_rank,0,global_comm);
      free(all_data_b);
      free(b);

      //F_Send(mw_results,(f->res_sz)*work_recvd,MPI_BYTE,proxy_master_rank,0,global_comm,FAIL_RATE);
      //F_Send(b,sizeof(int)*2*total_workers,MPI_BYTE,proxy_master_rank,0,global_comm,FAIL_RATE);


     // F_Send(b,sizeof(int)*(2*total_workers),MPI_BYTE,proxy_master_rank,0,global_comm,FAIL_RATE);
    }
    for(int i=0;i<total_workers;i++){
      if(workers[i]->idle ==1 && workers[i]->alive ==1){
        //send work
        printf("came in here, id no %d\n",workers[i]->rank);
        if(Front(q) == NULL)
          break;

        work_allocation_t * ser_work = Front(q);
        printf("new master %d inside\n",rank);
        F_Send(ser_work->work_data, f->work_sz,MPI_BYTE,workers[i]->rank,0,global_comm,FAIL_RATE);
         //MPI_Send(ser_work->work_data, f->work_sz,MPI_BYTE,workers[i]->rank,0,global_comm);
        total_free_workers--;
        workers[i]->current_work_allocation = ser_work;
        workers[i]->idle =0;
        workers[i]->last_work_received_at = MPI_Wtime();
        printf("my rank %d, assign work to worker %d\n",rank,workers[i]->rank);
        Dequeue(q);
      }
    }

    char result_buf[f->res_sz];
    MPI_Request request;
    int received = 1;

    worker_t * due_worker = next_due_worker(workers,total_workers);
    if(due_worker == NULL){
      continue;
    }
    printf("my rank %d,due worker's rank is %d\n",rank,due_worker->rank);
    while(MPI_Wtime()<due_worker->last_work_received_at+TIMEOUT && total_free_workers < total_alive_workers){
      if(received==1){
        MPI_Irecv(result_buf,f->res_sz,MPI_BYTE,MPI_ANY_SOURCE,0,global_comm,&request);
      }

      MPI_Test(&request,&received,&status);
        if(received ==1){
          worker_t * sender = find_worker_by_rank(status.MPI_SOURCE, workers);
          if(sender->alive ==1){
            //printf("size of mw_results %d and size of pos %d\n",f->res_sz*totalWorks,work_recvd*f->res_sz);
            memcpy(((char *)mw_results) + work_recvd*f->res_sz, result_buf,f->res_sz);
            work_recvd++;
            sender->idle =1;
            total_free_workers++;
            printf("my rank %d,receive result from alive worker %d\n",rank,status.MPI_SOURCE);
          }
          else{
            printf("my rank %d,receive result from dead worker %d\n",rank,status.MPI_SOURCE );
          }
        }
    }
    if(received != 1)
      MPI_Cancel(&request);

    if(due_worker->idle == 0){
      printf("my rank %d,due worker times out\n",rank);
      due_worker->alive =0;
      total_alive_workers--;
      Enqueue(q,due_worker->current_work_allocation);
    }

    if(total_alive_workers != 0){
      printf("-----after waiting for results\n");
      printf("total free:%d\n", total_free_workers);    
      printf("total alive:%d\n", total_alive_workers);
      printf("my rank %d,queue size is %d\n",rank,queue_size(q));
    }
  }

  if(total_alive_workers == 0 && queue_size(q) != 0)
    printf("total failure!!!\n");
  else
    f->result(totalWorks,mw_results);

  return 0;
}


int terminate_process(MPI_Comm global_comm,int process_id){
  char b = 'k';
  MPI_Send(&b,1,MPI_BYTE, process_id, TAG_TERMINATE, global_comm);
  return 0;

}



int proxy_master(MPI_Comm global_comm, int argc, char** argv, struct mw_api_spec *f){
  int size;
  printf("Hello, I am proxy master\n");

  int rank;
  MPI_Comm_rank(global_comm, &rank);


  MPI_Comm_size(global_comm, &size );  
  mw_work_t ** works = f->create(argc, argv);
  queue *q = malloc(sizeof(queue));
   q->rear = NULL;
   q->front = NULL;

int total_workers = size-2;

  //adding all work into the queue
 int totalWorks=0;
  while(*(works+totalWorks) != NULL){
    int count = 1;
    int work_id = totalWorks;//(int *)malloc(sizeof(int)*count);
    totalWorks+=count;
  }


  int received_work_ids[totalWorks];
  for(int i=0;i<totalWorks;i++){
    received_work_ids[i] = -1;
  }

  worker_t *workers[total_workers];
  for(int i=2;i<size;i++){
    worker_t *worker = malloc(sizeof(worker_t));
    worker->rank=i;
    worker->alive=1;
    work_allocation_t* dat = (work_allocation_t *)malloc(sizeof(work_allocation_t ));
    worker->current_work_allocation = dat;
    workers[i-2] = worker;
  }


   
  char *b = malloc(sizeof(int)*2*total_workers);
  MPI_Status status;
  MPI_Request request;
  int received = 0;
  //MPI_Request request2;
  long last_recvd_time = MPI_Wtime();

  char * all_data_buff = malloc(sizeof(int)*2*total_workers+f->res_sz*totalWorks);

  int is_alive = 1;
  mw_result_t * mw_results = malloc(f->res_sz*totalWorks);
 

  while(is_alive){  


    MPI_Irecv(all_data_buff, sizeof(int)*2*total_workers+f->res_sz*totalWorks,MPI_BYTE,0,0,global_comm,&request);
    while(MPI_Wtime()<last_recvd_time+4*TIMEOUT){
        MPI_Test(&request,&received,&status);
        if(received==1){

          memcpy(b, all_data_buff,sizeof(int)*2*total_workers);
          memcpy(mw_results,all_data_buff+sizeof(int)*2*total_workers, f->res_sz*totalWorks);
          last_recvd_time = MPI_Wtime();
          update_worker_status(b,workers,total_workers);
          update_recvd_works(received_work_ids,totalWorks,workers,total_workers);
          //temp_count +=1;
          printf("receiving works fine\n");
          break;
        }

    }
    
    if(!received){
        is_alive = 0;
        MPI_Cancel(&request);
        printf("killing master!!! hahaha!!!\n");
        terminate_process(global_comm,0);
        //kill_master(prime_comm);
    }

  }

for(int i=0;i<totalWorks;i++){
  if(received_work_ids[i] == 1)
      continue;

    char * b = serialize_works(1, works + i, f->work_sz);
    int work_id = i;//(int *)malloc(sizeof(int)*count);
    work_allocation_t* dat = (work_allocation_t *) malloc(sizeof(work_allocation_t ));
    dat->work_data = b;
    dat->work_id = work_id;
    dat->size = 1;
    Enqueue(q,dat);
  }


int total_alive_workers = 0;
for(int i=0;i<total_workers;i++){
  if(workers[i]->alive ==1)
    total_alive_workers +=1;
}


  get_work_done(rank,global_comm,q,workers, mw_results ,total_workers, total_alive_workers, total_alive_workers, f, totalWorks);
  free(works);
  free(all_data_buff);
  free(mw_results);
  free(b);
  MPI_Abort(MPI_COMM_WORLD,0);

  return 0;  


  
  
  //MPI_Recv(mw_results,)

  //recieve results, worker_t from master
  // if doesn't recieve back in after a certain period of time then take over.
}








int master(MPI_Comm global_comm, int argc, char** argv, struct mw_api_spec *f )
{ 
  int size;
  printf("Hello, I am a master\n");

  int  rank;
  MPI_Comm_rank(global_comm, &rank);

  MPI_Comm_size(global_comm, &size );  
  mw_work_t ** works = f->create(argc, argv);

  queue *q = malloc(sizeof(queue));
   q->rear = NULL;
   q->front = NULL;

  int proxy_master_rank = 1;

  //adding all work into the queue
  int totalWorks=0;
  while(*(works+totalWorks) != NULL){
    int count = 1;
    char * b = serialize_works(count, works + totalWorks, f->work_sz);
    int work_id = totalWorks;//(int *)malloc(sizeof(int)*count);

    totalWorks+=count;

    work_allocation_t* dat = (work_allocation_t *)malloc(sizeof(work_allocation_t ));
    dat->work_data = b;
    dat->work_id = work_id;
    dat->size = count;
    Enqueue(q,dat);
  }


  mw_result_t * mw_results = malloc(f->res_sz*totalWorks);
  
   int total_workers = size-2;
  int total_free_workers = total_workers,total_alive_workers = total_workers;
  worker_t *workers[total_workers];
  for(int i=2;i<size;i++){
    worker_t *worker = malloc(sizeof(worker_t));
    worker->rank=i;
    worker->idle=1;
    worker->alive=1;
    workers[i-2] = worker;
  }

  MPI_Status status;
  int work_recvd = 0;

  get_work_done(rank,global_comm,q,workers, mw_results ,total_workers, total_free_workers, total_alive_workers, f, totalWorks);
  
  free(mw_results);
  free(works);
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
    int curr_master = 0;
    int proxy_master = 1;

    MPI_Comm_rank(global_comm, &rank);
    MPI_Request request;
    long last_comm_time = MPI_Wtime();
    //long last_work_sent_at = MPI_Wtime();
    int comm_flag = 0;

    
    while(1){
      char * buf = malloc(f->work_sz*nWorks);
     // printf("rank %d and curr master %d\n",rank, curr_master );
     MPI_Iprobe(MPI_ANY_SOURCE, MPI_ANY_TAG, global_comm, &comm_flag, &status);

     if(comm_flag==1 && (status.MPI_SOURCE ==0 || status.MPI_SOURCE == 1)){
      MPI_Recv(buf,f->work_sz*nWorks,MPI_BYTE,MPI_ANY_SOURCE,0,global_comm,&status);
     //  while(MPI_Wtime()<last_comm_time+TIMEOUT){
     //    MPI_Test(&request,&comm_flag,&status);
     //    if(comm_flag==1){
     //      printf("rank %d curr_master %d status source %d\n",rank,curr_master, status.MPI_SOURCE);
     //      break;
     //    }

     //  }
     // if(comm_flag==0){
     //    MPI_Cancel(&request);
     //    curr_master = proxy_master;
     //    last_comm_time = MPI_Wtime();
     //    free(buf);
     //    continue;
     //  }

      //// execute works

      char result_buf[f->res_sz];  
      //for(int i = 0;i<nWorks;i++){
        mw_work_t * work = deserialize_work(buf,f->work_sz);//buf+i*(f->work_sz)
        mw_result_t * result = f->compute(work);
        memcpy(result_buf, result,f->res_sz);
        F_Send(result_buf,f->res_sz,MPI_BYTE,status.MPI_SOURCE,0,global_comm,FAIL_RATE);
        //F_Send(result_buf,f->res_sz,MPI_BYTE,curr_master,0,global_comm,FAIL_RATE);
       
        last_comm_time = MPI_Wtime();

        free(work);
        free(buf);
    }
    }
    return 0;
}
