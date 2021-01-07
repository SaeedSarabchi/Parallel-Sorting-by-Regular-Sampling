/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil ; -*- */
/*
 *  (C) 2001 by Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

#include <stdio.h>
#include "mpi.h"
#include <stdlib.h>
#include <pthread.h>
#include <sys/time.h>


#define MASTER if (myid == 0)


int   NUM_GLOBALINPUTDATA;
int numprocs;
int* PartitionDisps;
int* PartitionCnt;
int   MAX_NUMBER;



int compare (const void * a, const void * b)
{
  return ( *(int*)a - *(int*)b );
}

void FillRegularSample(int *SampleArray, int* inputDataArray, int ThreadID, int size)
{

	int LocalSize = NUM_GLOBALINPUTDATA/(numprocs);
	// Omega in (n/p^2) in the original paper
	int Omega =  LocalSize/numprocs;
	//printf("\n moega:%i \n",Omega);
	int counter;
	for(counter=0;counter<= size; counter = counter + Omega)
	{
		//printf(" \n sample array[%i] =: %i  \n",(ThreadID)*(NUM_THREADS)+((counter-StartIndex)/Omega), inputDataArray[counter] );
		SampleArray[((counter)/Omega)] = inputDataArray[counter];
	}

}
void ExtractPivots(int *inputPivotArray, int* SamplingArray)
{
	int counter;
	int Phi = numprocs/2;
	for(counter= (Phi+numprocs-1); counter<=numprocs*numprocs; counter = counter + numprocs)
		inputPivotArray[(counter-(Phi+numprocs-1))/numprocs] = SamplingArray[counter];
}
int  PivotPostionByBinarySearch(int* inputDataArray,int StartIndex,int Endindex,int SearchKey )
{
int first = StartIndex;
int last = Endindex;
int middle = first + (last-first)/2;

while (first <= last) {
  if (inputDataArray[middle] < SearchKey)
	 first = middle + 1;
  else if (inputDataArray[middle] == SearchKey) {
	  while(middle+1<=(Endindex)&& inputDataArray[middle+1]==inputDataArray[middle])
		  middle++;

	 return middle;
  }
  else
	 last = middle - 1;

  middle = first + (last-first)/2;
}
if (first > last)
{
   if(first>(Endindex))
	   first=(Endindex);
  return first;
}
return -1;

}

void FillPartition(int* PivotArray, int* inputDataArray, int ThreadID, int StartIndex, int Endindex)
{
	// In this Function we Partition each Thread's Data to (#Threads-1) Chunks of Data.

	int PivotCounter;
	PartitionDisps[0]=0;
	for(PivotCounter=0; PivotCounter<numprocs-1; PivotCounter++)
	{
		PartitionDisps[PivotCounter+1]=PivotPostionByBinarySearch(inputDataArray,StartIndex,Endindex,PivotArray[PivotCounter]);
		PartitionCnt[PivotCounter]= PartitionDisps[PivotCounter+1]-PartitionDisps[PivotCounter];
	}
	PartitionCnt[numprocs-1]=Endindex-PartitionDisps[numprocs-1];
}
int FindMinIndex(int *List)
{
	int index=0;
	int count;
	for(count=0; count<=numprocs-2; count++)
		if(List[index]>List[count+1])
			index=count+1;
	return index;

}
void MergePartialOrderedLists(int* InputDataArray, int size, int ThreadID, int* RcvCnt )
{
int* Outputptrs[numprocs];
int counter;
for(counter=0;counter<numprocs;counter++)
{
Outputptrs[counter]=(int *)malloc((RcvCnt[counter])*sizeof(int));
}
int innercnt;
int tmpcnt=0;
for( counter = 0; counter < numprocs; counter++ )
{
		{
			for(innercnt=0;innercnt<RcvCnt[counter];innercnt++)
			{
				Outputptrs[counter][innercnt]=InputDataArray[tmpcnt];
				tmpcnt++;
			}

		}
}
int buffer[numprocs];
int* PointersToThreadData[numprocs];
for( counter = 0; counter < numprocs; counter++ )
{

	PointersToThreadData[counter]=Outputptrs[counter];
	if(RcvCnt[counter]!=0)
		buffer[counter]=*(PointersToThreadData[counter]);
	else
		buffer[counter]=MAX_NUMBER;
}


int MinIndex;

for (counter=0; counter<size; counter++)
{
	MinIndex=FindMinIndex(buffer);
	InputDataArray[counter]=*(PointersToThreadData[MinIndex]);
	PointersToThreadData[MinIndex]++;
	if(Outputptrs[MinIndex]+RcvCnt[MinIndex]-1>=PointersToThreadData[MinIndex])
		buffer[MinIndex]=*(PointersToThreadData[MinIndex]);
	else
		buffer[MinIndex]= MAX_NUMBER;
}
//char tempStr[1000];


 /*sprintf(tempStr,"\n\n OutputPTr Numbers in Process %d are: ",ThreadID);
for( counter = 0; counter < numprocs; counter++ )
			{
			sprintf(tempStr+strlen(tempStr), " \n ptr%i :-",counter);
				for(innercnt=0;innercnt<RcvCnt[counter];innercnt++)
					sprintf(tempStr+strlen(tempStr), "%i-",Outputptrs[counter][innercnt]);
			}
		printf(tempStr);*/
for( counter = 0; counter < numprocs; counter++ )
{
	free(Outputptrs[counter]);
}
}
int main( int argc, char *argv[] )
{

    int    myid;
    double stop, start;
    double phase_stop, phase_start;
    double t_phase1, t_phase2, t_phase3, t_phase4, t_total;


     NUM_GLOBALINPUTDATA=atol(argv[1]);
     //MAX_NUMBER =NUM_GLOBALINPUTDATA;
      MAX_NUMBER =2147483647;

    MPI_Init( 0, 0 );
    MPI_Comm_size(MPI_COMM_WORLD,&numprocs);
     MPI_Comm_rank(MPI_COMM_WORLD,&myid);


     int* InputData ;
     int* OutputData ;
     int* GeneratedData;
     int* tempdata ;
     int RegularSample[numprocs];
     int* GatheredRegularSample;
     int Pivots[numprocs-1];
     char tempStr[1000];


     int counter;



     MASTER
     {


    	    GeneratedData = (int *)malloc((NUM_GLOBALINPUTDATA)*sizeof(int));
    	    //tempdata =  (int *)malloc((NUM_GLOBALINPUTDATA)*sizeof(int));
    		for( counter  = 0; counter < NUM_GLOBALINPUTDATA; counter++ )
    		{
    			GeneratedData[counter] = random() %MAX_NUMBER;
    			//tempdata[counter]=GeneratedData[counter];
    		}
     }


     int LocalSize = NUM_GLOBALINPUTDATA/numprocs;
     int scounts[numprocs];
     int displs[numprocs];

     for(counter=0;counter<numprocs;counter++)
     {
    	 displs[counter]=counter*LocalSize;

    	 if(counter!= numprocs-1)
    		 scounts[counter]=LocalSize;
    	 else
    		 scounts[counter]=NUM_GLOBALINPUTDATA-displs[counter];
     }


     InputData= (int *)malloc((scounts[myid])*sizeof(int));


     MPI_Scatterv(
    		 GeneratedData,
    		 scounts,
    		 displs,
    		 MPI_INT,
    		 InputData,
    		 scounts[myid],
    		 MPI_INT,
    		 0,
    		 MPI_COMM_WORLD);

   	//for capturing start and end time
   	//startTiming
      MPI_Barrier(MPI_COMM_WORLD);
      MASTER
      {
             start = MPI_Wtime();
      }

  	// Phase 1 :
  	// Sorting Chunks of Data which is Related to the Process
      MASTER
      {
             phase_start = MPI_Wtime();
      }


		qsort(InputData,scounts[myid], sizeof(int), compare);

		// Performing the Regular Sampling
		FillRegularSample(RegularSample, InputData, myid, scounts[myid]);





	     MPI_Barrier(MPI_COMM_WORLD);
	     MASTER
	     {
	            phase_stop = MPI_Wtime();
	            t_phase1=phase_stop-phase_start;
	     }
	     // End of Phase 1
	 	/*
	 	Phase 2
	 	Gathering and Sorting Regular 	Sample, Extracting Pivots and Broadcasting them
	 	*/
	     MASTER
	     {
	            phase_start = MPI_Wtime();
	     }
	     MASTER
	     {
	    	 GatheredRegularSample = (int *)malloc(( numprocs*numprocs)*sizeof(int));
	     }
	     MPI_Gather(
	    	RegularSample,
	         numprocs,
	         MPI_INT,
	         GatheredRegularSample,
	         numprocs,
	         MPI_INT,
	         0,
	         MPI_COMM_WORLD);

	     MASTER
	     {
	    	 qsort(GatheredRegularSample,numprocs*numprocs, sizeof(int), compare);
	    	 ExtractPivots(Pivots, GatheredRegularSample);
	     }
	     MPI_Bcast(
	    		 Pivots,
	    		 numprocs-1,
	    		 MPI_INT,
	    		 0,
	    		 MPI_COMM_WORLD);



	     MPI_Barrier(MPI_COMM_WORLD);
	     MASTER
	     {
	            phase_stop = MPI_Wtime();
	            t_phase2=phase_stop-phase_start;
	     }
	 	// End of Phase 2


	 	/*
	 	Phase 3 :
	 	Partitioning the data and Exchanging the Data
	 	*/
	     MASTER
	     {
	            phase_start = MPI_Wtime();
	     }
	     PartitionDisps =(int *)malloc((numprocs)*sizeof(int));
	     PartitionCnt =(int *)malloc((numprocs)*sizeof(int));

	     FillPartition(Pivots, InputData, myid, 0, scounts[myid]);
	     int Rcvdispls[numprocs];

/*MPI_Alltoall(
		PartitionDisps,
		1,
		MPI_INT,
		Rcvdispls,
		1,
		MPI_INT,
		MPI_COMM_WORLD);*/

int RcvCnt[numprocs];
MPI_Alltoall(
		PartitionCnt,
		1,
		MPI_INT,
		RcvCnt,
		1,
		MPI_INT,
		MPI_COMM_WORLD);







int sum=0;
for(counter=0;counter<numprocs;counter++)
	sum+=RcvCnt[counter];
OutputData =(int *)malloc((sum)*sizeof(int));

Rcvdispls[0]=0;
for( counter=0;counter<numprocs-1;counter++)
{
	Rcvdispls[counter+1]=Rcvdispls[counter]+ RcvCnt[counter];
}

MPI_Alltoallv(
		InputData,
		PartitionCnt,
		PartitionDisps,
		MPI_INT,
		OutputData,
		RcvCnt,
		Rcvdispls,
		MPI_INT,
		MPI_COMM_WORLD);

MPI_Barrier(MPI_COMM_WORLD);
	     MASTER
	     {
	            phase_stop = MPI_Wtime();
	            t_phase3=phase_stop-phase_start;
	     }

	     //END of Phase 3
/*
//TESTING
			sprintf(tempStr+strlen(tempStr),"\nNumbers in Process %d are: ",myid);

			for( counter = 0; counter < scounts[myid]; counter++ )
		 		{
					sprintf(tempStr+strlen(tempStr), "%i-",InputData[counter]);
		 		}
				sprintf(tempStr+strlen(tempStr),"\nRegular Sample in Process %d are: ",myid);

			for( counter = 0; counter < numprocs; counter++ )
				{
				sprintf(tempStr+strlen(tempStr), "%i-",RegularSample[counter]);
				}
			sprintf(tempStr+strlen(tempStr),"\n");
			printf(tempStr);
			 MPI_Barrier(MPI_COMM_WORLD);

			MASTER
			{

				sprintf(tempStr,"\nGathered Regular Sample is: ");
				for( counter = 0; counter <numprocs*numprocs; counter++ )
					{
					sprintf(tempStr+strlen(tempStr), "%i-",GatheredRegularSample[counter]);
					}
				//printf(tempStr);

						sprintf(tempStr+strlen(tempStr),"\n Extracted Pivots are: ");
						for( counter = 0; counter <numprocs-1; counter++ )
							{
							sprintf(tempStr+strlen(tempStr), "%i-",Pivots[counter]);
							}
						printf(tempStr);
			}


			     MPI_Barrier(MPI_COMM_WORLD);


			     sprintf(tempStr,"\n\n PartitionDisps in Process %d are: ",myid);

			     	     				for( counter = 0; counter < numprocs; counter++ )
			     	     			 		{
			     	     						sprintf(tempStr+strlen(tempStr), "%i-",PartitionDisps[counter]);
			     	     			 		}
			     	     					sprintf(tempStr+strlen(tempStr),"\n Rcvdispls in Process %d are: ",myid);
			     	     					for( counter = 0; counter < numprocs; counter++ )
			     	     				 		{
			     	     							sprintf(tempStr+strlen(tempStr), "%i-",Rcvdispls[counter]);
			     	     				 		}
			     	     					sprintf(tempStr+strlen(tempStr),"\n PartitionCnt in Process %d are: ",myid);
			     	     						for( counter = 0; counter < numprocs; counter++ )
			     	     					 		{
			     	     								sprintf(tempStr+strlen(tempStr), "%i-",PartitionCnt[counter]);
			     	     					 		}
			     	     						sprintf(tempStr+strlen(tempStr),"\n RcvCnt in Process %d are: ",myid);
			     	     							for( counter = 0; counter < numprocs; counter++ )
			     	     						 		{
			     	     									sprintf(tempStr+strlen(tempStr), "%i-",RcvCnt[counter]);
			     	     						 		}

			     	     					printf(tempStr);



			     MPI_Barrier(MPI_COMM_WORLD);

*/

	     /*
 	 	 sprintf(tempStr,"\n\n OutputPTr Numbers in Process %d are: ",myid);

	     int innercnt;
	     		for( counter = 0; counter < numprocs; counter++ )
	     	 		{
	     			sprintf(tempStr+strlen(tempStr), " \n ptr%i :-",counter);
	     				for(innercnt=0;innercnt<RcvCnt[counter];innercnt++)
	     					sprintf(tempStr+strlen(tempStr), "%i-",Outputptrs[counter][innercnt]);
	     	 		}
	     		printf(tempStr);*/
	     // END TESTING
	     		MPI_Barrier(MPI_COMM_WORLD);

	     			     	/*
Phase 4
Merging the Partitions by Merging Partial OrderedLists
*/
MASTER
{
	phase_start = MPI_Wtime();
}

MergePartialOrderedLists(OutputData, sum, myid, RcvCnt);

/*sprintf(tempStr,"\nNumbers in Process %d are: ",myid);

for( counter = 0; counter < scounts[myid]; counter++ )
		{
		sprintf(tempStr+strlen(tempStr), "%i-",OutputData[counter]);
		}
	printf("\n%s",tempStr);*/


MPI_Barrier(MPI_COMM_WORLD);
MASTER
{
	phase_stop = MPI_Wtime();
	t_phase4=phase_stop-phase_start;
}
//End of Phase 4

//Finalization

int sizes[numprocs];
MPI_Allgather(
&sum,
1,
MPI_INT,
sizes,
1,
MPI_INT,
MPI_COMM_WORLD);

int sizeDspl[numprocs];
sizeDspl[0]=0;
for(counter=0;counter<numprocs-1;counter++)
{
 sizeDspl[counter+1]=sizeDspl[counter]+sizes[counter];
}

 MPI_Gatherv(
		 OutputData,
		 sum,
		 MPI_INT,
		 GeneratedData,
		 sizes,
		 sizeDspl,
		 MPI_INT,
		 0,
		 MPI_COMM_WORLD
		 );


MASTER
{

stop = MPI_Wtime();
t_total=stop-start;
printf("\n%f",t_total );
//Time Analysis :
printf("\n%f",t_phase1/t_total);
printf("\n%f",t_phase2/t_total);
printf("\n%f",t_phase3/t_total);
printf("\n%f\n",t_phase4/t_total);

}

/*
//CORRECTION TEST
MASTER
{
		qsort(tempdata,NUM_GLOBALINPUTDATA,sizeof(int),compare);
		int DiffNum=0;
		for(counter=0;counter<NUM_GLOBALINPUTDATA;counter++)
			if(tempdata[counter]!=GeneratedData[counter])
				DiffNum++;
		printf("\n DIFFNUM: %i \n", DiffNum);


		sprintf(tempStr,"\n OUTPUT DATA: ");
		for( counter = 0; counter < NUM_GLOBALINPUTDATA; counter++ )
	 		{
				sprintf(tempStr+strlen(tempStr), "%i-",GeneratedData[counter]);
	 		}

			printf(tempStr);
			sprintf(tempStr,"\n ");
			printf("\n QSORTED DATA: ");
			for( counter = 0; counter < NUM_GLOBALINPUTDATA; counter++ )
		 		{
					sprintf(tempStr+strlen(tempStr), "%i-",tempdata[counter]);
		 		}
				sprintf(tempStr+strlen(tempStr),"\n ");
				printf(tempStr);

		free(tempdata);
}
*/


//Free the mallocs
MASTER
{
free(GeneratedData);
free(GatheredRegularSample);

}
free(InputData);
free(OutputData);
free(PartitionDisps);
free(PartitionCnt);


MPI_Finalize();
return 0;
}
