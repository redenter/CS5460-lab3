#include <stdio.h>
#include "mw_api.h"
#include "math.h"
#include <gmp.h>
#include <string.h>
#include "factorizer.h"

#define MAX_DIGITS 40
//#define MAX_FACTORS_PER_WORK 10000000000

struct userdef_work_t{
  char val[MAX_DIGITS];
  char lower_bound[MAX_DIGITS];
  char upper_bound[MAX_DIGITS];
};


struct userdef_result_t{
  int n;
  mpz_t * factors;
};


serial_t * serialize_result(mw_result_t * res){
  serial_t * ret = malloc(sizeof(serial_t));
  size_t data_size=0;

  for(int i =0;i<res->n;i++){
    data_size += mpz_sizeinbase(*(res->factors +i), 10);
  }
  
  size_t allocated_size = data_size +2*res->n;
  ret->size = allocated_size;
  ret->data = malloc(allocated_size);

  size_t offset=0;
  for(int i =0;i<res->n;i++){
    char * str = mpz_get_str(NULL,10,*(res->factors+i));
    memcpy(ret->data + offset,str,strlen(str)+1);
    offset += strlen(str) +1;
  }

  //fill the remaining bytes with tabs
  for(;offset < allocated_size;offset++)
    *(ret->data +offset) = '\t';

  return ret;

}

mw_result_t * deserialize_result(serial_t * res_data){
  mw_result_t * result = malloc(sizeof(mw_result_t));
  int n =0;
  for(int i =0;i<res_data->size;i++){
    if(*(res_data->data +i) == '\0')
      n++;
  }
  result->n = n;
  result->factors = malloc(sizeof(mpz_t)*n);
  int start_pos =0;
  int k=0;
  for(int i=0;i<res_data->size;i++){
    if(*(res_data->data+i) == '\0'){
      mpz_init(*(result->factors+k));
      mpz_set_str(*(result->factors+k),res_data->data+start_pos,10);
      start_pos = i+1;
      k++;
    }
  }
  return result;
}

mw_result_t *do_work(mw_work_t *work){
  mw_result_t *mw_result = malloc(sizeof(mw_result_t));
  mpz_t val;
  mpz_init(val);
  mpz_set_str(val,work->val,10);

  mpz_t lower_bound;
  mpz_init(lower_bound);
  mpz_set_str(lower_bound,work->lower_bound,10);

  mpz_t upper_bound;
  mpz_init(upper_bound);
  mpz_set_str(upper_bound,work->upper_bound,10);
  
  int factor_count;
  struct factor_node * factor_nodes = find_factors(lower_bound,upper_bound,val,&factor_count);

  if(factor_nodes == NULL)
    return NULL;

  mw_result->factors = malloc(sizeof(mpz_t)*factor_count);
  mw_result->n = factor_count;

  for(int i=0;i<factor_count;i++){
    memcpy((mw_result->factors)+i,factor_nodes->factor,sizeof(mpz_t));
    factor_nodes = factor_nodes->next;
  }

  return mw_result;
}


mw_work_t **create_work(int argc,char **argv){

  char val[MAX_DIGITS];
  strncpy(val,argv[1],MAX_DIGITS);
  mpz_t mp_val;
  mpz_init(mp_val);
  mpz_set_str(mp_val,val,10);

  mpz_t mp_sqrtn;
  mpz_init(mp_sqrtn);  
  mpz_sqrt (mp_sqrtn, mp_val);
  unsigned long int sqrtn = mpz_get_ui(mp_sqrtn);

  unsigned long int max_factors_per_work; 
  max_factors_per_work = strtoul(argv[2],NULL,10);

  printf("maximum number of factors to test per job: %lu\n",max_factors_per_work);
  unsigned long int nWorks = (sqrtn + max_factors_per_work -1)/max_factors_per_work;
  unsigned long int remain = sqrtn % max_factors_per_work;
  
  mw_work_t ** works = malloc(sizeof(mw_work_t*) * (nWorks+1));

  mpz_t f;
  mpz_init(f);
  mpz_set_str(f,"2",10);
  for(int i=0;i<nWorks;i++){
    mw_work_t *wrk = malloc(sizeof(mw_work_t));
    strcpy(wrk->val,val);
    unsigned long int count = i == nWorks -1 && remain != 0 ? remain : max_factors_per_work;

    char *lower_bound = mpz_get_str(NULL,10,f);
    mpz_add_ui(f,f,count-1);
    char *upper_bound = mpz_get_str(NULL,10,f);
    mpz_add_ui(f,f,1);

    strcpy(wrk->lower_bound,lower_bound);    
    strcpy(wrk->upper_bound,upper_bound);

    *(works+i) = wrk;
  }
  *(works+nWorks) = NULL;
  return works;
}


int * process_results(int sz, mw_result_t *res){
  int factors_count=0;

  for (int i =0;i<sz;i++){
    factors_count += (res+i)->n;
  }
  printf("The number has %d factors\n",factors_count);
  return 1;
}



int main(int argc, char **argv)
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
  MPI_Finalize ();

  return 0;

}