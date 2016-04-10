#include <stdio.h>
#include "mpi.h"
#include "mw_api.h"

#define MAX_MESSAGE_SIZE_IN_BYTE 500000
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

int master( MPI_Comm global_comm, int argc, char** argv, struct mw_api_spec *f )
{ 
    int size;
    printf("Hello, I am a master\n");

    MPI_Comm_size(global_comm, &size );  
    mw_work_t ** works = f->create(argc, argv);


    int totalWorks=0;
    while(*(works+totalWorks) != NULL)
      totalWorks++;

    // send works to workers
    int a = totalWorks/(size-1);
    int remain = totalWorks %(size-1);
    int offset=0;
    for(int worker_rank =1;worker_rank<size;worker_rank++){
      int count = worker_rank <= remain ? a + 1 : a;
      if(count == 0) 
        continue;
      char * b = serialize_works(count, works + offset, f->work_sz);
      MPI_Send(&count,1,MPI_INT,worker_rank,0,global_comm);
      send_message(b,count*f->work_sz,worker_rank,0,global_comm);
      offset += count;
      free(b);
    }

    //// collect results
    mw_result_t * mw_results = malloc(f->res_sz*totalWorks);
    char result_buf[f->res_sz];
    MPI_Status status;

    offset=0;
    for(int worker_rank=1;worker_rank<size;worker_rank++){
      int count = worker_rank <= remain ? a + 1 : a;

      for(int i =0;i<count;i++){
        MPI_Recv(result_buf,f->res_sz,MPI_BYTE,worker_rank,0,global_comm,&status);
        memcpy(((char *)mw_results) + offset*f->res_sz, result_buf,f->res_sz);
        offset++;
      }
    }
    
    f->result(totalWorks,mw_results);

    free(works);
    free(mw_results);
}


int slave(MPI_Comm global_comm, struct mw_api_spec *f)
{
    int  rank;
    MPI_Status status;
    int nWorks;

    MPI_Comm_rank(global_comm, &rank);
    MPI_Recv(&nWorks,1,MPI_INT,0,0,global_comm,&status);
    char * buf = malloc(f->work_sz*nWorks);
    receive_message(buf,nWorks*(f->work_sz),0,0,global_comm);

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
    
    return 0;
}
