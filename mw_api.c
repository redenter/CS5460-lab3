#include <stdio.h>
#include "mpi.h"
#include "mw_api.h"
#include <time.h>

#define MAX_MESSAGE_SIZE_IN_BYTE 500000
/* run master-worker */
void MW_Run (int argc, char **argv, struct mw_api_spec *f){

  int rank, size;
  MPI_Comm mw_comm;

  MPI_Comm_rank( MPI_COMM_WORLD, &rank );
  //MPI_Comm_split( MPI_COMM_WORLD, rank == 0, 0, &mw_comm );
  if (rank == 0){ 
    double start_time,end_time;
    start_time = MPI_Wtime();
    master( MPI_COMM_WORLD, argc, argv, f);
    end_time = MPI_Wtime();

    double t = end_time-start_time;
    printf("Total time is %f microseconds\n",t*1000000);
  }
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

/// send a big message in multiple small chunks
int send_message(char * buf, int size,int dest, int tag, MPI_Comm comm){
  int n = size / MAX_MESSAGE_SIZE_IN_BYTE;
  for(int i =0;i<n;i++){
    MPI_Send(buf+(MAX_MESSAGE_SIZE_IN_BYTE*i), MAX_MESSAGE_SIZE_IN_BYTE,MPI_BYTE,dest,tag,comm);
  }

  if(MAX_MESSAGE_SIZE_IN_BYTE*n != size)
    MPI_Send(buf+MAX_MESSAGE_SIZE_IN_BYTE*n, size -MAX_MESSAGE_SIZE_IN_BYTE*n,MPI_BYTE,dest,tag,comm);
}

/// receive a big message in multiple small chunks
int receive_message(char * buf, int size, int source, int tag, MPI_Comm comm){
  MPI_Status status;
  int n = size / MAX_MESSAGE_SIZE_IN_BYTE;
  for(int i =0;i<n;i++){
    MPI_Recv(buf+(MAX_MESSAGE_SIZE_IN_BYTE*i), MAX_MESSAGE_SIZE_IN_BYTE,MPI_BYTE,source,tag,comm,&status);
  }

  if(MAX_MESSAGE_SIZE_IN_BYTE*n != size)
    MPI_Recv(buf+MAX_MESSAGE_SIZE_IN_BYTE*n, size-MAX_MESSAGE_SIZE_IN_BYTE*n,MPI_BYTE,source,tag,comm,&status);
}





int master( MPI_Comm global_comm, int argc, char** argv, struct mw_api_spec *f)
{ 
    int size;


    printf("Hello, I am a master\n");

    MPI_Comm_size(global_comm, &size );  
    mw_work_t ** works = f->create(argc, argv);



    int totalWorks=0;
    while(*(works+totalWorks) != NULL)
      totalWorks++;


 //// send works to workers
    int a = totalWorks/(size-1);
    int remain = totalWorks %(size-1);
    int offset=0;
    int busy_workers_count=0;


    //added code
    serial_t * mapping; 

 //// variables required for collecting results
    MPI_Status status;
    char ** data[size];       // data sent by all workers
    int * result_sizes[size]; // sizes of individual results sent by each worker
    int result_counts[size];  // number of results sent by each worker
    int total_results = 0; 

    //added code
    int pos = 0;


    for(int worker_rank =1;worker_rank<size;worker_rank++){
      int count = worker_rank <= remain ? a + 1 : a;
      char * b = serialize_works(count, works + offset, f->work_sz);
     //added code
      mapping->size = count;
      mapping->data = b;
     
      MPI_Send(&count,1,MPI_INT,worker_rank,0,global_comm);
      send_message(b,count*f->work_sz,worker_rank,0,global_comm);
      offset += count;
      if(count != 0)
        busy_workers_count++;
      free(b);
    }

    //// free up memory occupied by works
    for(mw_work_t ** iter=works; *iter != NULL;iter++)
      free(*iter);

//added code
  while(q.size()!=0){
      //receive count from any source

      MPI_Recv(result_counts +pos,1,MPI_INT,MPI_ANY_SOURCE,0,global_comm,MPI_STATUS_IGNORE);
      total_results += result_counts[pos];
      result_sizes[pos] = malloc(sizeof(int) * result_counts[pos]);
      data[pos] = malloc(sizeof(char *)*result_counts[pos]);

      for(int j=0;j<result_counts[worker_rank];j++){
        MPI_Recv(result_sizes[pos]+j,1,MPI_INT,status.MPI_SOURCE,0,global_comm,&status);
      }

       for(int j=0;j<result_counts[pos];j++){
        int _size = *(result_sizes[pos]+j);
        *(data[pos] +j) = malloc(_size);
        MPI_Recv(*(data[pos]+j),_size,MPI_BYTE,status.MPI_SOURCE,0,global_comm,&status);
      }
      pos++;

      //send topmost data from queue to this source
      serial_t *dat = q.top();

      MPI_Send(&dat->size,1,MPI_INT,worker_rank,0,global_comm);
      send_message(dat->data,(dat->size)*f->work_sz,worker_rank,0,global_comm);

    //in case of failure, add to queue.

  }





    //// collect results

    
    // for(int worker_rank=1;worker_rank<=busy_workers_count;worker_rank++){   

    //   MPI_Recv(result_counts +worker_rank,1,MPI_INT,worker_rank,0,global_comm,MPI_STATUS_IGNORE);
    //   total_results += result_counts[worker_rank];
    //   result_sizes[worker_rank] = malloc(sizeof(int) * result_counts[worker_rank]);
    //   data[worker_rank] = malloc(sizeof(char *)*result_counts[worker_rank]);

    //   for(int j=0;j<result_counts[worker_rank];j++){
    //     MPI_Recv(result_sizes[worker_rank]+j,1,MPI_INT,worker_rank,0,global_comm,&status);
    //   }

    //   for(int j=0;j<result_counts[worker_rank];j++){
    //     int _size = *(result_sizes[worker_rank]+j);
    //     *(data[worker_rank] +j) = malloc(_size);
    //     MPI_Recv(*(data[worker_rank]+j),_size,MPI_BYTE,worker_rank,0,global_comm,&status);
    //   }
    // }



    mw_result_t * mw_results = malloc(f->res_sz*total_results);
    
    int i=0;
    for(int worker_rank=1;worker_rank<=busy_workers_count;worker_rank++){

      for(int j=0;j<result_counts[worker_rank];j++){
        serial_t s_data = {result_sizes[worker_rank][j], *(data[worker_rank]+j)};

        mw_result_t * _result = f->deserialize_result(&s_data);
        memcpy(((char *)mw_results)+ i*f->res_sz, _result, f->res_sz);
        free(_result);
        i++;
      }
    }
    
    /// process results
    f->result(total_results,mw_results);
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
    int n_results=0;
    serial_t ** serial_results = malloc(sizeof(serial_t *) * nWorks);
    char result_buf[f->res_sz];    
    for(int i = 0;i<nWorks;i++){
      mw_work_t * work = deserialize_work(buf+i*(f->work_sz),f->work_sz);
      mw_result_t * result = f->compute(work);
      if(result == NULL)
        continue;


      *(serial_results + n_results) = f->serialize_result(result);      
      n_results++;
      free(work);
      free(result);
    }

    //// sending results to master
    MPI_Send(&n_results,1,MPI_INT,0,0,global_comm);
    for(int i =0;i<n_results;i++){
      MPI_Send(&((*(serial_results+i))->size),1,MPI_INT,0,0,global_comm);
    }

    for(int i =0;i<n_results;i++){
      MPI_Send((*(serial_results+i))->data,(*(serial_results+i))->size,MPI_BYTE,0,0,global_comm);        
    }

    free(serial_results);
    free(buf);
    return 0;
}
