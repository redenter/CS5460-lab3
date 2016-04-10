#include <stdio.h>
#include "mw_api.h"

struct userdef_work_t{
  int w;
  int q;
};

struct userdef_result_t{
  int sum;
};


mw_work_t ** create_work (int argc, char **argv){
  static mw_work_t w1 = {25,5};  
  static mw_work_t w2 = {30,40};
  mw_work_t ** works = malloc(sizeof(mw_work_t *)*11);
  for(int i=0;i<10;i++)
    *(works +i) = i % 2 == 0 ? &w1 : &w2;
  *(works+10) =NULL;
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
  return  result;
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
  MW_Run (argc, argv, &f);
  MPI_Finalize ();

  return 0;
}


