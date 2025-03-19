/***************** Transaction class **********************/
/*** Implements methods that handle Begin, Read, Write, ***/
/*** Abort, Commit operations of transactions. These    ***/
/*** methods are passed as parameters to threads        ***/
/*** spawned by Transaction manager class.              ***/
/**********************************************************/

/* Spring 2025: CSE 4331/5331 Project 2 : Tx Manager */

/* Required header files */
#include <stdio.h>
#include <stdlib.h>
#include <sys/signal.h>
#include "zgt_def.h"
#include "zgt_tm.h"
#include "zgt_extern.h"
#include <unistd.h>
#include <iostream>
#include <fstream>
#include <pthread.h>

#define READ_MODE 'R'
#define WRITE_MODE 'W'
#define EXCLUSIVE_LOCK 'X'
#define SHARED_LOCK 'S'
#define COMMIT 'c'
#define ABORT 'a'
#define NOT_BEGIN_TX ' '


extern void *start_operation(long, long);  //start an op with mutex lock and cond wait
extern void *finish_operation(long);        //finish an op with mutex unlock and con signal

extern void *do_commit_abort_operation(long, char);   //commit/abort based on char value
extern void *process_read_write_operation(long, long, int, char);

extern zgt_tm *ZGT_Sh;			// Transaction manager object

/* Transaction class constructor */
/* Initializes transaction id and status and thread id */
/* Input: Transaction id, status, thread id */

zgt_tx::zgt_tx( long tid, char Txstatus, char type, pthread_t thrid){
  this->lockmode = (char)' ';   // default
  this->Txtype = type;          // R = read only, W=Read/Write
  this->sgno =1;
  this->tid = tid;
  this->obno = -1;              // set it to a invalid value
  this->status = Txstatus;
  this->pid = thrid;
  this->head = NULL;
  this->nextr = NULL;
  this->semno = -1;             // init to an invalid sem value
}

/* Method used to obtain reference to a transaction node      */
/* Inputs the transaction id. Makes a linear scan over the    */
/* linked list of transaction nodes and returns the reference */
/* of the required node if found. Otherwise returns NULL      */

zgt_tx* get_tx(long tid1){  
  zgt_tx *txptr, *lastr1;
  
  if(ZGT_Sh->lastr != NULL){	// If the list is not empty
      lastr1 = ZGT_Sh->lastr;	// Initialize lastr1 to first node's ptr
      for  (txptr = lastr1; (txptr != NULL); txptr = txptr->nextr)
	    if (txptr->tid == tid1) 		// if required id is found									
	       return txptr; 
      return (NULL);			// if not found in list return NULL
   }
  return(NULL);				// if list is empty return NULL
}

/* Method that handles "BeginTx tid" in test file     */
/* Inputs a pointer to transaction id, obj pair as a struct. Creates a new  */
/* transaction node, initializes its data members and */
/* adds it to transaction list */

void *begintx(void *arg){
  //intialise a transaction object. Make sure it is 
  //done after acquiring the semaphore for the tm and making sure that 
  //the operation can proceed using the condition variable. When creating
  //the tx object, set the tx to TR_ACTIVE and obno to -1; there is no 
  //semno as yet as none is waiting on this tx.
  
  struct param *node = (struct param*)arg;// get tid and count
  start_operation(node->tid, node->count); 
    zgt_tx *tx = new zgt_tx(node->tid,TR_ACTIVE, node->Txtype, pthread_self());	// Create new tx node
 
    // Writes the Txtype to the file.
  
    zgt_p(0);				// Lock Tx manager; Add node to transaction list
  
    tx->nextr = ZGT_Sh->lastr;
    ZGT_Sh->lastr = tx;   
    zgt_v(0); 			// Release tx manager 
  fprintf(ZGT_Sh->logfile, "T%ld\t%c \tBeginTx\n", node->tid, node->Txtype);	// Write log record and close
    fflush(ZGT_Sh->logfile);
  finish_operation(node->tid);
  pthread_exit(NULL);				// thread exit
}

/* Method to handle Readtx action in test file    */
/* Inputs a pointer to structure that contans     */
/* tx id and object no to read. Reads the object  */
/* if the object is not yet present in hash table */
/* or same tx holds a lock on it. Otherwise waits */
/* until the lock is released */

void *readtx(void *arg){
  struct param *node = (struct param*)arg;// get tid and objno and count

  // write your code
  start_operation(node->tid, node->count); // wait for previous thread of same transaction to end
  process_read_write_operation(node->tid, node->obno, node->count, READ_MODE);
  finish_operation(node->tid);
  pthread_exit(NULL); // thread exit
}


void *writetx(void *arg){ //do the operations for writing; similar to readTx
  struct param *node = (struct param*)arg;	// struct parameter that contains
  
  // write your code
  start_operation(node->tid, node->count); // wait for previous thread of same transaction to end
  process_read_write_operation(node->tid, node->obno, node->count, WRITE_MODE);
  finish_operation(node->tid);
  pthread_exit(NULL); // thread exit
}

// common method to process read/write: Just a suggestion

void *process_read_write_operation(long tid, long obno, int count, char mode)
{
  // write your code
  zgt_p(0);  // Lock transaction manager
  zgt_tx *tx = get_tx(tid);
  zgt_v(0);  // Unlock transaction manager

  if(tx == NULL) {
    printf(":::Tx with tid: %ld does not exist.\n", tid);
    fflush(stdout);
    fprintf(ZGT_Sh->logfile, "Tx with tid: %ld does not exist. skipping operation\n", tid);
    fflush(ZGT_Sh->logfile);
    return NULL;
  }

  // First, check if transaction has been aborted
  if(tx->status == TR_ABORT) {
    printf("Transaction %ld has been aborted, skipping operation.\n", tid);
    fflush(stdout);
    return NULL;
  }

  // Try to acquire the lock with a maximum number of retries
  int max_retries = 10;  // Increased retry count
  int retries = 0;
  int result = -1;
  char lock_type = (mode == READ_MODE) ? SHARED_LOCK : EXCLUSIVE_LOCK;

  while(result == -1 && retries < max_retries) {
    // Try to get the lock
    result = tx->set_lock(tid, tx->sgno, obno, count, lock_type);
    
    if(result == -1) {
      retries++;
      // Sleep briefly before retrying to avoid tight loops
      usleep(50000);  // 50ms
      
      // Check if transaction has been aborted during waiting
      zgt_p(0);
      tx = get_tx(tid);
      if(tx == NULL || tx->status == TR_ABORT) {
        zgt_v(0);
        printf("Transaction %ld has been aborted while waiting for lock.\n", tid);
        fflush(stdout);
        return NULL;
      }
      zgt_v(0);
    }
  }

  if(result == -1) {
    printf("WARNING: Max retries exceeded for transaction %ld on object %ld\n", tid, obno);
    fflush(stdout);
    fprintf(ZGT_Sh->logfile, "WARNING: Max retries exceeded for transaction %ld on object %ld\n", tid, obno);
    fflush(ZGT_Sh->logfile);
    return NULL;  // Don't proceed if we couldn't get the lock
  }

  // Double-check transaction status before performing the operation
  zgt_p(0);
  tx = get_tx(tid);
  if(tx == NULL || tx->status == TR_ABORT) {
    zgt_v(0);
    return NULL;
  }
  zgt_v(0);
  
  // Now perform the actual read/write operation
  tx->perform_read_write_operation(tid, obno, mode);
  
  return NULL;
}

void *aborttx(void *arg)
{
  struct param *node = (struct param*)arg;// get tid and count  

  // write your code
  start_operation(node->tid, node->count); // wait for previous thread of same transaction to end
  do_commit_abort_operation(node->tid, ABORT);
  finish_operation(node->tid);
  pthread_exit(NULL); // thread exit
}

void *committx(void *arg)
{
  struct param *node = (struct param*)arg;// get tid and count

  // write your code
  start_operation(node->tid, node->count); // wait for previous thread of same transaction to end
  do_commit_abort_operation(node->tid, COMMIT);
  finish_operation(node->tid);
  pthread_exit(NULL); // thread exit
}

//suggestion as they are very similar

// called from commit/abort with appropriate parameter to do the actual
// operation. Make sure you give error messages if you are trying to
// commit/abort a non-existent tx

void *do_commit_abort_operation(long t, char status){
  // write your code
  zgt_p(0);  // Lock transaction manager
  zgt_tx *tx = get_tx(t);
  if(tx == NULL) {
    printf(":::Tx with tid: %ld does not exist.\n", t);
    fflush(stdout);
    fprintf(ZGT_Sh->logfile, "Tx with tid: %ld does not exist. skipping operation\n", t);
    fflush(ZGT_Sh->logfile);
    zgt_v(0);
    return NULL;
  }

  // Set transaction status
  if(status == COMMIT) {
    tx->status = TR_END;
    fprintf(ZGT_Sh->logfile, "T%ld\t%c\tCommitTx\n", t, NOT_BEGIN_TX);
    printf("Transaction %ld committed.\n", t);
  } else if(status == ABORT) {
    tx->status = TR_ABORT;
    fprintf(ZGT_Sh->logfile, "T%ld\t%c\tAbortTx\n", t, NOT_BEGIN_TX);
    printf("Transaction %ld aborted.\n", t);
  }
  fflush(ZGT_Sh->logfile);
  fflush(stdout);

  // Store the semaphore number for use after lock release
  int tx_semno = tx->tid;  // Use transaction ID as semaphore number

  // Free all locks held by this transaction
  tx->free_locks();
  
  // Release transaction from transaction list
  tx->remove_tx();

  // Wake up any transactions waiting on this transaction's locks
  int num_waiting = zgt_nwait(tx_semno);
  printf("Waking up %d transactions waiting on transaction %ld\n", num_waiting, t);
  fflush(stdout);
  
  for(int i = 0; i < num_waiting; i++) {
    zgt_v(tx_semno);  // Release waiting transactions
  }

  zgt_v(0);  // Unlock transaction manager
  return NULL;
}

int zgt_tx::remove_tx ()
{
  //remove the transaction from the TM
  
  zgt_tx *txptr, *lastr1;
  lastr1 = ZGT_Sh->lastr;
  for(txptr = ZGT_Sh->lastr; txptr != NULL; txptr = txptr->nextr){	// scan through list
	  if (txptr->tid == this->tid){		// if correct node is found          
		 lastr1->nextr = txptr->nextr;	// update nextr value; done
		 //delete this;
         return(0);
	  }
	  else lastr1 = txptr->nextr;			// else update prev value
   }
  fprintf(ZGT_Sh->logfile, "Trying to Remove a Tx:%ld that does not exist\n", this->tid);
  fflush(ZGT_Sh->logfile);
  printf("Trying to Remove a Tx:%ld that does not exist\n", this->tid);
  fflush(stdout);
  return(-1);
}

/* this method sets lock on objno1 with lockmode1 for a tx*/

int zgt_tx::set_lock(long tid1, long sgno1, long obno1, int count, char lockmode1){
  // write your code
  zgt_p(0);  // Lock transaction manager
  
  // Check if this transaction already has a lock on this object
  zgt_hlink *ob = ZGT_Ht->find(sgno1, obno1);
  zgt_hlink *myLock = ZGT_Ht->findt(tid1, sgno1, obno1);

  // If we already have a lock on this object, just return success
  if(myLock != NULL) {
    this->status = TR_ACTIVE;
    zgt_v(0);
    return 0;
  }

  // If object is not locked by anyone, we can get the lock
  if(ob == NULL) {
    // Add new lock to hash table
    if(ZGT_Ht->add(this, sgno1, obno1, lockmode1) == -1) {
      printf("Error adding lock to hash table\n");
      fflush(stdout);
      zgt_v(0);
      return -1;
    }
    this->status = TR_ACTIVE;
    zgt_v(0);
    return 0;
  }

  // If object is locked by another transaction, check lock compatibility
  if(ob->tid != tid1) {
    // If both locks are shared, they can coexist
    if(lockmode1 == SHARED_LOCK && ob->lockmode == SHARED_LOCK) {
      // Both transactions want shared access, compatible
      if(ZGT_Ht->add(this, sgno1, obno1, lockmode1) == -1) {
        printf("Error adding shared lock to hash table\n");
        fflush(stdout);
        zgt_v(0);
        return -1;
      }
      this->status = TR_ACTIVE;
      zgt_v(0);
      return 0;
    }
    
    // If we're here, locks are not compatible (either we or they want exclusive)
    // Need to wait for the lock to be released
    this->status = TR_WAIT;
    this->semno = ob->tid;  // Set semaphore to wait on the holding transaction
    
    // Get handle to the transaction that holds the lock
    zgt_tx *otherTx = get_tx(ob->tid);
    
    // Notify we're waiting
    printf("Transaction %ld waiting for lock on object %ld held by transaction %ld\n", 
           tid1, obno1, ob->tid);
    fflush(stdout);
    
    zgt_v(0);  // Unlock transaction manager
    zgt_p(this->semno);  // Wait on the semaphore
    
    // After waking up, we need to retry from the beginning
    return -1;
  }

  // If we get here, something unexpected happened
  zgt_v(0);
  return -1;
}

int zgt_tx::free_locks()
{
  // This part frees all locks owned by the transaction and prints each unique object once
  
  // First, create an array to keep track of objects we've processed
  long processed_objs[MAX_ITEMS] = {0};
  int num_processed = 0;
  
  // Print all objects held by this transaction (without duplicates)
  zgt_hlink* temp = head;
  while(temp != NULL) {
    // Check if we've already processed this object
    bool already_processed = false;
    for(int i = 0; i < num_processed; i++) {
      if(processed_objs[i] == temp->obno) {
        already_processed = true;
        break;
      }
    }
    
    // Only print and process if not already done
    if(!already_processed) {
      fprintf(ZGT_Sh->logfile, "%ld : %d, ", temp->obno, ZGT_Sh->objarray[temp->obno]->value);
      processed_objs[num_processed++] = temp->obno;
    }
    
    temp = temp->nextp;
  }
  fprintf(ZGT_Sh->logfile, "\n");
  fflush(ZGT_Sh->logfile);
  
  // Now remove all locks from the hash table
  temp = head;
  while(temp != NULL) {
    zgt_hlink* next = temp->nextp;
    
    if(ZGT_Ht->remove(this, 1, (long)temp->obno) == 1) {
      printf(":::ERROR:node with tid:%ld and objno:%ld was not found for deleting", this->tid, temp->obno);
      fflush(stdout);
    }
#ifdef TX_DEBUG
    else {
      printf("\n:::Hash node with Tid:%ld, obno:%ld lockmode:%c removed\n",
             temp->tid, temp->obno, temp->lockmode);
      fflush(stdout);
    }
#endif
    
    temp = next;
  }
  
  return(0);
}

// CURRENTLY Not USED
// USED to COMMIT
// remove the transaction and free all associate dobjects. For the time being
// this can be used for commit of the transaction.

int zgt_tx::end_tx()  
{
  zgt_tx *linktx, *prevp;
  
  // USED to COMMIT 
  //remove the transaction and free all associate dobjects. For the time being 
  //this can be used for commit of the transaction.
  
  linktx = prevp = ZGT_Sh->lastr;
  
  while (linktx){
    if (linktx->tid  == this->tid) break;
    prevp  = linktx;
    linktx = linktx->nextr;
  }
  if (linktx == NULL) {
    printf("\ncannot remove a Tx node; error\n");
    fflush(stdout);
    return (1);
  }
  if (linktx == ZGT_Sh->lastr) ZGT_Sh->lastr = linktx->nextr;
  else {
    prevp = ZGT_Sh->lastr;
    while (prevp->nextr != linktx) prevp = prevp->nextr;
    prevp->nextr = linktx->nextr;    
  }
}

// currently not used
int zgt_tx::cleanup()
{
  return(0);
  
}

// routine to print the tx list
// TX_DEBUG should be defined in the Makefile to print
void zgt_tx::print_tm(){
  
  zgt_tx *txptr;
  
#ifdef TX_DEBUG
  printf("printing the tx  list \n");
  printf("Tid\tTxType\tThrid\t\tobjno\tlock\tstatus\tsemno\n");
  fflush(stdout);
#endif
  txptr=ZGT_Sh->lastr;
  while (txptr != NULL) {
#ifdef TX_DEBUG
    printf("%ld\t%c\t%ld\t%ld\t%c\t%c\t%d\n", txptr->tid, txptr->Txtype, txptr->pid, txptr->obno, txptr->lockmode, txptr->status, txptr->semno);
    fflush(stdout);
#endif
    txptr = txptr->nextr;
  }
  fflush(stdout);
}

//need to be called for printing
void zgt_tx::print_wait(){

  //route for printing for debugging
  
  printf("\n    SGNO        TxType       OBNO        TID        PID         SEMNO   L\n");
  printf("\n");
}

void zgt_tx::print_lock(){
  //routine for printing for debugging
  
  printf("\n    SGNO        OBNO        TID        PID   L\n");
  printf("\n");
  
}

// routine to perform the actual read/write operation as described the project description
// based  on the lockmode

void zgt_tx::perform_read_write_operation(long tid,long obno, char lockmode){
  // write your code
  if(lockmode == READ_MODE) {
    // Read operation - decrement value by 4
    ZGT_Sh->objarray[obno]->value -= 4;
    fprintf(ZGT_Sh->logfile, "T%ld\t%c\tReadTx\t\t%ld:%d:%d\t\tReadLock\tGranted\t%c\n",
            tid, NOT_BEGIN_TX, obno, ZGT_Sh->objarray[obno]->value, ZGT_Sh->optime[tid], this->status);
  } else {
    // Write operation - increment value by 7
    ZGT_Sh->objarray[obno]->value += 7;
    fprintf(ZGT_Sh->logfile, "T%ld\t%c\tWriteTx\t\t%ld:%d:%d\t\tWriteLock\tGranted\t%c\n",
            tid, NOT_BEGIN_TX, obno, ZGT_Sh->objarray[obno]->value, ZGT_Sh->optime[tid], this->status);
  }
  fflush(ZGT_Sh->logfile);

  // Sleep for the operation time
  usleep(ZGT_Sh->optime[tid]);
}

// routine that sets the semno in the Tx when another tx waits on it.
// the same number is the same as the tx number on which a Tx is waiting
int zgt_tx::setTx_semno(long tid, int semno){
  zgt_tx *txptr;
  
  txptr = get_tx(tid);
  if (txptr == NULL){
    printf("\n:::ERROR:Txid %ld wants to wait on sem:%d of tid:%ld which does not exist\n", this->tid, semno, tid);
    fflush(stdout);
    exit(1);
  }
  if ((txptr->semno == -1)|| (txptr->semno == semno)){  //just to be safe
    txptr->semno = semno;
    return(0);
  }
  else if (txptr->semno != semno){
#ifdef TX_DEBUG
    printf(":::ERROR Trying to wait on sem:%d, but on Tx:%ld\n", semno, txptr->tid);
    fflush(stdout);
#endif
    exit(1);
  }
  return(0);
}

void *start_operation(long tid, long count){
  
  pthread_mutex_lock(&ZGT_Sh->mutexpool[tid]);	// Lock mutex[t] to make other
  // threads of same transaction to wait
  
  while(ZGT_Sh->condset[tid] != count)		// wait if condset[t] is != count
    pthread_cond_wait(&ZGT_Sh->condpool[tid],&ZGT_Sh->mutexpool[tid]);
  
}

// Otherside of teh start operation;
// signals the conditional broadcast

void *finish_operation(long tid){
  ZGT_Sh->condset[tid]--;	// decr condset[tid] for allowing the next op
  pthread_cond_broadcast(&ZGT_Sh->condpool[tid]);// other waiting threads of same tx
  pthread_mutex_unlock(&ZGT_Sh->mutexpool[tid]); 
}