#include <stdio.h>
#include "mpi.h"
#include "mw_api.h"
#include "queue.h"

#define MAX_MESSAGE_SIZE_IN_BYTE 500000
#define TIMEOUT 2000
/* run master-worker */
void MW_Run (int argc, char **argv, struct mw_api_spec *f){

  int rank, size;
  MPI_Comm mw_comm;

  MPI_Comm_rank( MPI_COMM_WORLD, &rank );
  //MPI_Comm_split( MPI_COMM_WORLD, rank == 0, 0, &mw_comm );
  if (rank == 0) 
    master( MPI_COMM_WORLD, argc, argv, f);
  else
    slave( MPI_COMM_WORLD, f);
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
    MPI_Send(buf+MAX_MESSAGE_SIZE_IN_BYTE*n, size -MAX_MESSAGE_SIZE_IN_BYTE*n,MPI_BYTE,dest,tag,comm);    MPI_Send(buf+MAX_MESSAGE_SIZE_IN_BYTE*n, size -MAX_MESSAGE_SIZE_IN_BYTE*n,MPI_BYTE,dest,tag,comm);}

int receive_message(char * buf, int size, int source, int tag, MPI_Comm comm){
  MPI_Status status;
  int n = size / MAX_MESSAGE_SIZE_IN_BYTE;
  for(int i =0;i<n;i++){
    MPI_Recv(buf+(MAX_MESSAGE_SIZE_IN_BYTE*i), MAX_MESSAGE_SIZE_IN_BYTE,MPI_BYTE,source,tag,comm,&status);
  }

  if(MAX_MESSAGE_SIZE_IN_BYTE*n != size)
    MPI_Recv(buf+MAX_MESSAGE_SIZE_IN_BYTE*n, size-MAX_MESSAGE_SIZE_IN_BYTE*n,MPI_BYTE,source,tag,comm,&status);
}


//check. Can return -1
int min_time_idx(double * last_delivery_time,int * aliveWorkers,size){
   int idx = -1;
  for(int x=1;x<size;x++){
    if(*(aliveWorkers+x)==1){
      if(idx ==-1 || last_delivery_time[idx]>last_delivery_time[x] )
        idx = x;
      }
  }
  return idx;
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

      struct serial_t* dat = (struct serial_t *)malloc(sizeof(struct serial_t ));
      dat->data = b;
      dat->size = count;
      Enqueue(dat);
      }

  mw_result_t * mw_results = malloc(f->res_sz*totalWorks);
  


  // 1: worker is free 0: worker is busy
   int *freeWorkers = malloc(sizeof(int) *size);
   //1: worker is 
   int *aliveWorkers =  malloc(sizeof(int) *size);;
    MPI_Status status;
    int work_recvd = 0;

    struct serial_t ** sent_work = malloc(sizeof(struct serial_t*) * (size-1));


   double * last_delivery_time = malloc(sizeof(double) *size );;

   //assigning initial values
  for(int i=1;i<size;i++){
    *(freeWorkers+i) = 1;
    *(aliveWorkers+i) = 1;

  }

//while(queue_size()!=0){

  for(int i=1;i<size;i++){
    if(*(freeWorkers+i) ==1 && *(aliveWorkers+i)==1){
      //send work
      struct serial_t * ser_work = Front();
      //char * b = serialize_works(1, works + 1, f->work_sz);

      MPI_Send(ser_work->data, f->work_sz,MPI_BYTE,i,0,global_comm);

      *(sent_work+i) = ser_work;
      *(freeWorkers+i) = 0;
      *(last_delivery_time+i) = MPI_Wtime();

       printf("after mpi !!\n");

    }
  }
  char result_buf[f->res_sz];
  MPI_Request request;


    int flag = 1;

      printf("before loop %d\n",min_time_idx(last_delivery_time,aliveWorkers,size));

  while(MPI_Wtime()<last_delivery_time[min_time_idx(last_delivery_time,aliveWorkers,size)]+TIMEOUT){

       if(status.MPI_TAG==MPI_SUCCESS){
        printf("inside this area");
        work_recvd +=1;
        memcpy(((char *)mw_results) + work_recvd*f->res_sz, result_buf,f->res_sz);
        *(freeWorkers+status.MPI_SOURCE) = 0;
        *(last_delivery_time+status.MPI_SOURCE) = MPI_Wtime();
      }

      if(flag==1){
        MPI_Irecv(result_buf,f->res_sz,MPI_BYTE,MPI_ANY_SOURCE,0,global_comm,&request);
        printf("inside I recv cond");
      }
      
    MPI_Test(&request,&flag,&status);
  }
  if(flag == 0 ){
    Enqueue( *(sent_work+status.MPI_SOURCE));
    *(aliveWorkers+status.MPI_SOURCE) = 0;
}

  //recieve from any source

//}
printf("finished the process");
//
 f->result(totalWorks,mw_results);

    free(works);
    free(mw_results);



    // send works to workers
    // int a = totalWorks/(size-1);
    // int remain = totalWorks %(size-1);
    // int offset=0;
    // for(int worker_rank =1;worker_rank<size;worker_rank++){
    //   int count = worker_rank <= remain ? a + 1 : a;
    //   if(count == 0) 
    //     continue;
    //   char * b = serialize_works(count, works + offset, f->work_sz);
    //   MPI_Send(&count,1,MPI_INT,worker_rank,0,global_comm);
    //   send_message(b,count*f->work_sz,worker_rank,0,global_comm);
    //   offset += count;
    //   free(b);
    // }

    //// collect results

    // offset=0;
    // for(int worker_rank=1;worker_rank<size;worker_rank++){
    //   int count = worker_rank <= remain ? a + 1 : a;

    //   for(int i =0;i<count;i++){
    //     MPI_Recv(result_buf,f->res_sz,MPI_BYTE,worker_rank,0,global_comm,&status);
    //     memcpy(((char *)mw_results) + offset*f->res_sz, result_buf,f->res_sz);
    //     offset++;
    //   }
    // }
    
 return 0;  
}


int slave(MPI_Comm global_comm, struct mw_api_spec *f)
{
    int  rank;
    MPI_Status status;
    int nWorks=1;

    MPI_Comm_rank(global_comm, &rank);
  //  MPI_Recv(Works,1,MPI_INT,0,0,global_comm,&status);
    
    while(1){
    char * buf = malloc(f->work_sz*nWorks);
    MPI_Recv(buf,f->work_sz*nWorks,MPI_BYTE,0,0,global_comm,&status);
    //printf("recieved work. I am slave no %d\n",rank);
  //  receive_message(buf,nWorks*(f->work_sz),0,0,global_comm);

    //// execute works

    //mw_result_t ** results = malloc(sizeof(mw_result_t *) * nWorks);
    char result_buf[f->res_sz];    
    for(int i = 0;i<nWorks;i++){
      mw_work_t * work = deserialize_work(buf+i*(f->work_sz),f->work_sz);
      mw_result_t * result = f->compute(work);
      memcpy(result_buf, result,f->res_sz);
      MPI_Send(result_buf,f->res_sz,MPI_BYTE,0,0,global_comm);
//      *(results +i) = f->compute(work);
      free(work);
    }
    free(buf);
    }
    return 0;
}
