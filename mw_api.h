struct userdef_work_t; /* definition provided by user */
struct userdef_result_t; /* definition provided by user */
typedef struct userdef_work_t mw_work_t;
typedef struct userdef_result_t mw_result_t;

typedef struct serial_chunks{
  int size;
  char *data;
} serial_t;



struct mw_api_spec {
   mw_work_t **(*create) (int argc, char **argv);
      /* create work: return a NULL-terminated list of work. Return NULL if it fails. */

   int (*result) (int sz, mw_result_t *res);     
      /* process result. Input is a collection of results, of size sz. Returns 1 on success, 0 on failure. */

   mw_result_t *(*compute) (mw_work_t *work);   
      /* compute, returning NULL if there is no result, non-NULL if there is a result to be returned. */

   int work_sz, res_sz;
      /* size in bytes of the work structure and result structure, needed to send/receive messages */

   serial_t * (*serialize_result) (mw_result_t * res);

   mw_result_t * (*deserialize_result) (serial_t * res_data);
};

void MW_Run (int argc, char **argv, struct mw_api_spec *f);

