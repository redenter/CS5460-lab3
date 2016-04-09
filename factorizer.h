#include <stdio.h>
#include "math.h"
#include <gmp.h>
#include <string.h>


struct factor_node{
  mpz_t factor;
  struct factor_node *next;
};


struct node * find_factors(mpz_t from, mpz_t to, mpz_t val, int * count){

  mpz_t i;
  mpz_init(i);
  mpz_set(i,from);

  struct factor_node * head = NULL;
  struct factor_node * tail = NULL;
  
  if(count != NULL)
    *count =0;
  
  while(mpz_cmp(i,to)<=0){
    if(mpz_divisible_p(val,i) != 0){
      struct factor_node * node1 = malloc(sizeof(struct factor_node));
      mpz_init(node1->factor);
      mpz_set(node1->factor,i);

      if(head ==NULL){
        head = node1;
        tail = node1;
      }
      else{
        tail->next = node1;
        tail = tail->next; 
      }

      mpz_t q;
      mpz_init(q);
      mpz_cdiv_q(q,val,i);
      struct factor_node *node2 = malloc(sizeof(struct factor_node));
      mpz_init(node2->factor);
      mpz_set(node2->factor,q);

      node2->next = NULL;
      tail->next =node2;
      tail = tail->next;
      if(count != NULL)
        *count = (*count) +2;
    }    
    mpz_add_ui(i,i,1);
  } 
  return head; 
}
