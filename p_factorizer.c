#include <stdio.h>
#include "mw_api.h"
#include "math.h"
#include <gmp.h>
#include <string.h>
#include "factorizer.h"

#define MAX_DIGITS 40
#define MAX_FACTORS_PER_WORK 1000

struct userdef_work_t{
  char val[MAX_DIGITS];
  char lower_bound[MAX_DIGITS];
  char upper_bound[MAX_DIGITS];
};


struct userdef_result_t{
  char factors[2*MAX_FACTORS_PER_WORK][MAX_DIGITS];
};

mw_result_t *do_work(mw_work_t *work){
  mw_result_t *result = malloc(sizeof(mw_result_t));
  mpz_t val;
  mpz_init(val);
  mpz_set_str(val,work->val,10);

  mpz_t lower_bound;
  mpz_init(lower_bound);
  mpz_set_str(lower_bound,work->lower_bound,10);

  mpz_t upper_bound;
  mpz_init(upper_bound);
  mpz_set_str(upper_bound,work->upper_bound,10);
  struct factor_node * factor_nodes = find_factors(lower_bound,upper_bound,val);

  int i =0;
  struct factor_node * iter = factor_nodes;

  while(iter != NULL){
    strcpy(result->factors[i++],mpz_get_str(NULL,10,iter->factor));
    iter = iter->next;
  }

  for(;i<2*MAX_FACTORS_PER_WORK;i++)
    result->factors[i][0] = '\0';

  return result;
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

  unsigned long int nWorks = (sqrtn + MAX_FACTORS_PER_WORK -1)/MAX_FACTORS_PER_WORK;
  int remain = sqrtn % MAX_FACTORS_PER_WORK;
  
  mw_work_t ** works = malloc(sizeof(mw_work_t*) * (nWorks+1));

  mpz_t f;
  mpz_init(f);
  mpz_set_str(f,"2",10);
  for(int i=0;i<nWorks;i++){
    mw_work_t *wrk = malloc(sizeof(mw_work_t));
    strcpy(wrk->val,val);
    int count = i == nWorks -1 && remain != 0 ? remain : MAX_FACTORS_PER_WORK;

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
  for(int i=0;i<sz;i++){
    for(int j =0;j<2*MAX_FACTORS_PER_WORK;j++){
      if (strlen((res+i)->factors[j]) != 0)
        factors_count++;
    }
  }
  char results[factors_count][MAX_DIGITS];
  int c =0;
  for(int i=0;i<sz;i++){
    for(int j =0;j<2*MAX_FACTORS_PER_WORK;j++){
      if (strlen((res+i)->factors[j]) != 0){
        strcpy(results[c++],(res+i)->factors[j]);
      }
    }
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
  MW_Run (argc, argv, &f);
  MPI_Finalize ();

  return 0;

}