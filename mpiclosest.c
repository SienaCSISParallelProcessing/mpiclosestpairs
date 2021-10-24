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

// struct to encapsulate info about the tasks in the bag
typedef struct cptask {
  int num_vertices;
  char *filename;
} cptask;


// helper function to read only up to the number of vertices from a
// TMG file and return that number
int read_tmg_vertex_count(char *filename) {

  FILE *fp = fopen(filename, "r");
  if (!fp) {
    fprintf(stderr, "Cannot open file %s for reading.\n", filename);
    MPI_Abort(MPI_COMM_WORLD, 1);
  }

  // read over first line
  char temp[100];
  fscanf(fp, "%s %s %s", temp, temp, temp);
  
  // read number of vertices
  int nv;
  fscanf(fp, "%d", &nv);

  // that's all we need for now
  fclose(fp);

  return nv;
}

int main(int argc, char *argv[]) {
  
  int numprocs, rank;
  // how many jobs?
  int jobs_done = 0;
  
  // about how many distance calculations?
  long dcalcs = 0;

  int worker_rank;
  int num_tasks;

  int i;
  
  MPI_Init(&argc, &argv);
  MPI_Comm_size(MPI_COMM_WORLD, &numprocs);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);

  if (numprocs < 2) {
    fprintf(stderr, "Must have at least 2 processes\n");
    MPI_Abort(MPI_COMM_WORLD, 1);
  }

  // all parameters except argv[0] (program name) and argv[1] (input
  // ordering) will be filenames to load, so the number of tasks
  // is argc - 2
  num_tasks = argc - 2;
  
  // the rank 0 process is responsible for doling out work to all of
  // the other processes
  if (rank == 0) {
    
    if (argc < 3) {
      fprintf(stderr, "Usage: %s orig|alpha|size|random filenames\n", argv[0]);
      MPI_Abort(MPI_COMM_WORLD, 1);
    }

    // check for a valid ordering in argv[1];
    char *orderings[] = {
      "orig",
      "alpha",
      "size",
      "random"
    };
    int ordering = -1;
    for (i = 0; i < 4; i++) {
      if (strcmp(argv[1], orderings[i]) == 0) {
	ordering = i;
	break;
      }
    }
    if (ordering == -1) {
      fprintf(stderr, "Usage: %s orig|alpha|size|random filenames\n", argv[0]);
      MPI_Abort(MPI_COMM_WORLD, 1);
    }      

    printf("Have %d tasks to be done by %d worker processes\n", num_tasks,
	   (numprocs-1));

    // allocate and populate our "bag of tasks" array
    cptask **tasks = (cptask **)malloc(num_tasks*sizeof(cptask *));

    // add the first at pos 0, since we know there's at least one and
    // this will eliminate some special cases in our code below.
    tasks[0] = (cptask *)malloc(sizeof(cptask));
    tasks[0]->filename = argv[2];
    if (ordering == 2) {
      tasks[0]->num_vertices = read_tmg_vertex_count(argv[2]);
    }

    // get them all in
    for (i = 1; i < num_tasks; i++) {
      cptask *taski = (cptask *)malloc(sizeof(cptask));
      taski->filename = argv[i+2];
      int pos = i;
      int insertat;
      switch (ordering) {

      case 0:
	// original ordering as specified by argv
	tasks[i] = taski;
	break;
      

      case 1:
	// alphabetical order by filename
	while (pos > 0 && strcmp(taski->filename, tasks[pos-1]->filename) < 0) {
	  tasks[pos] = tasks[pos-1];
	  pos--;
	}
	tasks[pos] = taski;
	
	break;

      case 2:
	// order by size largest to smallest number of vertices
	taski->num_vertices = read_tmg_vertex_count(taski->filename);
	printf("Got %d nv for %s\n", taski->num_vertices, taski->filename);
	while (pos > 0 && taski->num_vertices >= tasks[pos-1]->num_vertices) {
	  tasks[pos] = tasks[pos-1];
	  pos--;
	}
	tasks[pos] = taski;

	break;

      case 3:
	// order randomly
        insertat = random()%(pos+1);
	while (pos > insertat) {
	  tasks[pos] = tasks[pos-1];
	  pos--;
	}
	tasks[pos] = taski;
	break;
      }
    }
    
    // what's the next task to be sent out? (index into tasks array)
    int next_task = 0;

    // how many worker processes are currently active?
    int active_workers = 0;
    
    // start by sending one message to assign a task to each other process,
    // and a terminate message to others if there are more processes than
    // tasks
    for (worker_rank = 1; worker_rank < numprocs; worker_rank++) {

      if (next_task < num_tasks) {
	printf("Sending job %s to %d\n", tasks[next_task]->filename,
	       worker_rank);
	MPI_Send(tasks[next_task]->filename,
		 strlen(tasks[next_task]->filename)+1, MPI_CHAR,
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
      if (next_task < num_tasks) {
	printf("Sending job %s to %d\n", tasks[next_task]->filename,
	       status.MPI_SOURCE);
	MPI_Send(tasks[next_task]->filename,
		 strlen(tasks[next_task]->filename)+1, MPI_CHAR,
		 status.MPI_SOURCE, 0, MPI_COMM_WORLD);
	next_task++;
      }
      else {
	printf("Sending termination to %d\n", status.MPI_SOURCE);
	MPI_Send("DONE", 5, MPI_CHAR, status.MPI_SOURCE, 1, MPI_COMM_WORLD);
	active_workers--;
      }
    }

    for (i = 0; i < num_tasks; i++) {
      free(tasks[i]);
    }
    free(tasks);
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
    // compute some stats
    int minjobs = jobs_per_proc[1];
    int maxjobs = jobs_per_proc[1];
    double avgjobs = 1.0*num_tasks/(numprocs-1);
    long mincalcs = dcalcs_per_proc[1];
    long maxcalcs = dcalcs_per_proc[1];
    long totalcalcs = dcalcs_per_proc[1];
    for (worker_rank = 2; worker_rank < numprocs; worker_rank++) {
      if (jobs_per_proc[worker_rank] < minjobs)
	minjobs = jobs_per_proc[worker_rank];
      if (jobs_per_proc[worker_rank] > maxjobs)
	maxjobs = jobs_per_proc[worker_rank];
      if (dcalcs_per_proc[worker_rank] < mincalcs)
	mincalcs = dcalcs_per_proc[worker_rank];
      if (dcalcs_per_proc[worker_rank] > maxcalcs)
	maxcalcs = dcalcs_per_proc[worker_rank];
      totalcalcs += dcalcs_per_proc[worker_rank];
    }
    printf("%d workers processed %d jobs with about %ld distance calculations\n",
	   (numprocs-1), num_tasks, totalcalcs);
    printf("Job balance: min %d, max %d, avg: %.2f\n", minjobs, maxjobs,
	   avgjobs);
    printf("Distance calculation balance: min %ld, max %ld, avg: %.2f\n",
	   mincalcs, maxcalcs, ((1.0*totalcalcs)/(numprocs-1)));
    for (worker_rank = 1; worker_rank < numprocs; worker_rank++) {
      printf("%d: %d job%s, %ld distance calculations, difference from avg: %.2f\n",
	     worker_rank, jobs_per_proc[worker_rank],
	     (jobs_per_proc[worker_rank] == 1 ? "" : "s"),
	     dcalcs_per_proc[worker_rank],
	     (dcalcs_per_proc[worker_rank]-((1.0*totalcalcs)/(numprocs-1))));
    }
  }
  
  MPI_Finalize();
  return 0;
}
