#include <stdio.h>
#include "mw_api.h"

struct userdef_work_t{
  int w;
  int q;
};

struct userdef_result_t{
  int sum;
};

serial_t * serialize_result(mw_result_t * res){
  serial_t * ret = malloc(sizeof(serial_t));
  size_t data_size=sizeof(int);
  ret->size = data_size;
  ret->data = malloc(data_size);
  memcpy(ret->data, &(res->sum),data_size);
  return ret;
}

mw_result_t * deserialize_result(serial_t * res_data){
  mw_result_t * result = malloc(sizeof(mw_result_t));
  memcpy(&(result->sum),res_data->data,sizeof(int));
  return result;
}


mw_work_t ** create_work (int argc, char **argv){
  int nworks = 100;
  mw_work_t ** works = malloc(sizeof(mw_work_t *)*(nworks+1));
  for(int i=0;i<nworks;i++){
//    *(works +i) = i % 2 == 0 ? w1 : w2;

    mw_work_t * wrk = malloc(sizeof(mw_work_t));
    if(i%2){
      wrk->w =25;
      wrk->q =5;
    }
    else{
      wrk->w =30;
      wrk->q =40;      
    }
    *(works+i) = wrk;
  }
  *(works+nworks) =NULL;
  return works;
}

int * process_results (int sz, mw_result_t *res){
  int sum=0;
  for(int i=0;i<sz;i++){
    sum += res->sum;
    res++;
  }
  printf("Sum is: %d\n",sum);
  return 1;
}

mw_result_t *do_work (mw_work_t *work){
  mw_result_t * result = malloc(sizeof(mw_result_t));
  result->sum = work->w + work->q;
  return result;
}   


int main(int argc, char **argv )
{

  struct mw_api_spec f;

  MPI_Init (&argc, &argv);

  f.create = create_work;
  f.result = process_results;
  f.compute = do_work;
  f.work_sz = sizeof (struct userdef_work_t);
  f.res_sz = sizeof (struct userdef_result_t);
  f.serialize_result = serialize_result;
  f.deserialize_result = deserialize_result;
  MW_Run (argc, argv, &f);

  // mw_result_t * res = malloc(sizeof(mw_result_t));
  // res->sum = 25;
  // serial_t * s_data = serialize_result(res);
  // printf("size is %d\n",s_data->size);

  // mw_result_t * org_res = deserialize_result(s_data);
  // printf("sum is %d\n",org_res->sum);

  MPI_Finalize ();

  return 0;
}


