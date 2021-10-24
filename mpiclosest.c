/*
  Bag of Tasks MPI implementation to find the closest pairs of waypoints
  in each of a set of METAL TMG graph files.

  Jim Teresco, Fall 2021
  Siena College
*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <mpi.h>

#include "tmggraph.h"

int main(int argc, char *argv[]) {

  int numprocs, rank;
  // how many jobs?
  int jobs_done = 0;
  
  // about how many distance calculations?
  long dcalcs = 0;

  int worker_rank;

  
  MPI_Init(&argc, &argv);
  MPI_Comm_size(MPI_COMM_WORLD, &numprocs);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);

  if (numprocs < 2) {
    fprintf(stderr, "Must have at least 2 processes\n");
    MPI_Abort(MPI_COMM_WORLD, 1);
  }
  
  // the rank 0 process is responsible for doling out work to all of
  // the other processes
  if (rank == 0) {
    
    if (argc < 2) {
      fprintf(stderr, "Usage: %s filenames\n", argv[0]);
      MPI_Abort(MPI_COMM_WORLD, 1);
    }

    printf("Have %d tasks to be done by %d worker processes\n", (argc-1),
	   (numprocs-1));
    
    // what's the next task to be sent out?
    int next_task = 1;

    // how many worker processes are currently active?
    int active_workers = 0;
    
    // start by sending one message to assign a task to each other process,
    // and a terminate message to others if there are more processes than
    // tasks
    for (worker_rank = 1; worker_rank < numprocs; worker_rank++) {

      if (next_task < argc) {
	printf("Sending job %s to %d\n", argv[next_task], worker_rank);
	MPI_Send(argv[next_task], strlen(argv[next_task])+1, MPI_CHAR,
		 worker_rank, 0, MPI_COMM_WORLD);
	next_task++;
	active_workers++;
      }
      else {
	printf("Sending termination to %d\n", worker_rank);
	MPI_Send("DONE", 5, MPI_CHAR, worker_rank, 1, MPI_COMM_WORLD);
      }
    }

    // now as long as there are active workers doing things, wait for a
    // response and send wither 
    while (active_workers > 0) {

      // get a response from anyone who's got one
      char response[1000];
      MPI_Status status;
      MPI_Recv(response, 1000, MPI_CHAR, MPI_ANY_SOURCE, 0,
	       MPI_COMM_WORLD, &status);
      printf("%d: %s\n", status.MPI_SOURCE, response);

      // send the next task or a termination to that process
      if (next_task < argc) {
	printf("Sending job %s to %d\n", argv[next_task], status.MPI_SOURCE);
	MPI_Send(argv[next_task], strlen(argv[next_task])+1, MPI_CHAR,
		 status.MPI_SOURCE, 0, MPI_COMM_WORLD);
	next_task++;
      }
      else {
	printf("Sending termination to %d\n", status.MPI_SOURCE);
	MPI_Send("DONE", 5, MPI_CHAR, status.MPI_SOURCE, 1, MPI_COMM_WORLD);
	active_workers--;
      }
    }
  }
  else {
    // all worker processes (all except rank 0)

    while (1) {

      // receive a message which is either a task (filename),
      // indicated by a message with tag 0, or a message to terminate,
      // indicated by a message with tag 1
      MPI_Status status;
      char task[100];
      MPI_Recv(task, 100, MPI_CHAR, 0, MPI_ANY_TAG, MPI_COMM_WORLD, &status);

      if (status.MPI_TAG == 1) break;  // all done

      tmg_graph *g = tmg_load_graph(task);
      if (g == NULL) {
	fprintf(stderr, "Could not create graph from file %s\n", task);
	MPI_Abort(MPI_COMM_WORLD, 1);
      }
  
      int v1, v2;
      double distance;
      char response[1000];

      // do it
      tmg_closest_pair(g, &v1, &v2, &distance);

      jobs_done++;
      long job_calcs = g->num_vertices;
      job_calcs *= g->num_vertices;
      job_calcs /= 2;
      dcalcs += job_calcs;
      
      sprintf(response,
	      "%s closest pair #%d %s (%.6f,%.6f) and #%d %s (%.6f,%.6f) distance %.15f",
	      task, v1, g->vertices[v1]->w.label,
	      g->vertices[v1]->w.coords.lat, g->vertices[v1]->w.coords.lng,
	      v2, g->vertices[v2]->w.label,
	      g->vertices[v2]->w.coords.lat, g->vertices[v2]->w.coords.lng,
	      distance);
      
      // report back result
      MPI_Send(response, strlen(response) + 1, MPI_CHAR, 0, 0, MPI_COMM_WORLD);
      
      tmg_graph_destroy(g);
    }
  }

  // gather result summaries
  int *jobs_per_proc = (int *)malloc(numprocs * sizeof(int));
  long *dcalcs_per_proc = (long *)malloc(numprocs * sizeof(long));
  MPI_Gather(&jobs_done, 1, MPI_INT, jobs_per_proc, 1, MPI_INT, 0,
	     MPI_COMM_WORLD);
  MPI_Gather(&dcalcs, 1, MPI_LONG, dcalcs_per_proc, 1, MPI_LONG, 0,
	     MPI_COMM_WORLD);

  if (rank == 0) {
    for (worker_rank = 1; worker_rank < numprocs; worker_rank++) {
      printf("%d: %d jobs, %ld distance calculations\n", worker_rank,
	     jobs_per_proc[worker_rank], dcalcs_per_proc[worker_rank]);
    }
  }
  
  MPI_Finalize();
  return 0;
}
