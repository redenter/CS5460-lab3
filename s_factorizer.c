#include <stdio.h>
#include "mw_api.h"
#include "math.h"
#include <gmp.h>
#include <string.h>
#include "factorizer.h"
#include "mpi.h"

int main(int argc, char **argv)
{

  double start_time,end_time;
  start_time = MPI_Wtime();

  mpz_t val;
  mpz_init(val);
  mpz_set_str(val,argv[1],10);

  mpz_t mp_sqrt;
  mpz_init(mp_sqrt);  
  mpz_sqrt (mp_sqrt, val);


  mpz_t i;
  mpz_init(i);
  mpz_set_ui(i,2);
  int count;
  struct factor_node * factors = find_factors(i,mp_sqrt,val,&count);

  printf("The number has %d factors\n",count);

  end_time = MPI_Wtime();
  double t = end_time-start_time;
  printf("Total time is %f microseconds\n",t*1000000);

  return 0;
}


