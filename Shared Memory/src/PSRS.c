/*
 ============================================================================
 Name        : PSRS.c
 Author      : sasa
 Version     :
 Copyright   : Your copyright notice
 Description : Hello World in C, Ansi-style
 ============================================================================
 */

#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <sys/time.h>

#define NUM_THREADS 4
#define NUM_GLOBALINPUTDATA 8000000
#define MAX_NUMBER 10000
//#define NUM_LOCALINPUTDATA NUM_GLOBALINPUTDATA/NUM_THREADS
#define MASTER  if(myId == 0)
#define BARRIER    barrier( myId, __LINE__ );

int GlobalInputData[NUM_GLOBALINPUTDATA];
int RegularSample[NUM_THREADS*NUM_THREADS];
int Pivots[NUM_THREADS-1];
int Partitions[NUM_THREADS][NUM_THREADS-1];


struct ThreadControlBlock {
	int id;
};

pthread_barrier_t   barrier; // barrier synchronization object

struct ThreadControlBlock TCB[NUM_THREADS];
pthread_t ThreadID[ NUM_THREADS ];


typedef struct LinkListNode
{
	int value;
	struct LinkListNode * Next;

} LinkListNode_t;

LinkListNode_t*  SortedThreadDataLinkList[NUM_THREADS];


struct timeval stop, start;


int compare (const void * a, const void * b)
{
  return ( *(int*)a - *(int*)b );
}

void FillRegularSample(int *SampleArray, int* inputDataArray, int ThreadID, int StartIndex, int EndIndex)
{
	int LocalSize = NUM_GLOBALINPUTDATA/(NUM_THREADS);
	// Omega in (n/p^2) in the original paper
	int Omega =  LocalSize/NUM_THREADS;
	//printf("\n moega:%i \n",Omega);
	int counter;
	for(counter=StartIndex;counter<= EndIndex; counter = counter + Omega)
	{
		//printf(" \n sample array[%i] =: %i  \n",(ThreadID)*(NUM_THREADS)+((counter-StartIndex)/Omega), inputDataArray[counter] );
		SampleArray[(ThreadID)*(NUM_THREADS)+((counter-StartIndex)/Omega)] = inputDataArray[counter];
	}

}
void ExtractPivots(int *inputPivotArray, int* SamplingArray)
{
	int counter;
	int Phi = NUM_THREADS/2;
	for(counter= (Phi+NUM_THREADS-1); counter<=NUM_THREADS*NUM_THREADS; counter = counter + NUM_THREADS)
		inputPivotArray[(counter-(Phi+NUM_THREADS-1))/NUM_THREADS] = SamplingArray[counter];
}
void FillPartition(int* PivotArray, int* inputDataArray, int ThreadID, int StartIndex, int Endindex)
{
	// In this Function we Partition each Thread's Data to (#Threads-1) Chunks of Data.
	int InputDataCounter=StartIndex;
	int PivotCounter=0;
	while(InputDataCounter<=Endindex && PivotCounter<NUM_THREADS-1)
	{
		if(inputDataArray[InputDataCounter]<=Pivots[PivotCounter])
			InputDataCounter++;
		else
		{
			Partitions[ThreadID][PivotCounter]=InputDataCounter;

			PivotCounter++;
		}
	}
	if(PivotCounter<(NUM_THREADS-1))
	{

		int i;
		for(i=PivotCounter;i<NUM_THREADS-1; i++)
			Partitions[ThreadID][i]=InputDataCounter-1;
	}

}
int FindMinIndex(int *List)
{
	int index=0;
	int count;
	for(count=0; count<=NUM_THREADS-2; count++)
		if(List[index]>List[count+1])
			index=count+1;
	return index;

}
void MergePartialOrderedLists(int* InputDataArray, int ThreadID)
{
	int LocalSize=NUM_GLOBALINPUTDATA/NUM_THREADS;
	int counter;
	int StartIndex;
	int EndIndex;
	int ElementCount=0;
	int StartEnds[NUM_THREADS][2];
	int buffer[NUM_THREADS];
	int PointersToThreadData[NUM_THREADS];
	for (counter=0; counter<NUM_THREADS; counter++)
	{
		if(ThreadID!=0)
			StartIndex=Partitions[counter][ThreadID-1];
		else
			StartIndex = counter * LocalSize;

		if(ThreadID!=NUM_THREADS-1)
		{
			EndIndex=Partitions[counter][ThreadID]-1;
		}
		else
			if (counter!= NUM_THREADS-1)
				EndIndex=(counter+1) * LocalSize-1;
			else
				EndIndex=NUM_GLOBALINPUTDATA-1;

		StartEnds[counter][0]=StartIndex;
		StartEnds[counter][1]=EndIndex;
		buffer[counter]=InputDataArray[StartIndex];
		PointersToThreadData[counter]=StartIndex;
		ElementCount = ElementCount+(EndIndex-StartIndex)+1;
	}

	int MinIndex;
	SortedThreadDataLinkList[ThreadID]= malloc(sizeof(LinkListNode_t));
	LinkListNode_t* CurrentLinkListPointer=SortedThreadDataLinkList[ThreadID];

	for (counter=0; counter<ElementCount; counter++)
	{
		MinIndex=FindMinIndex(buffer);
		CurrentLinkListPointer->value=InputDataArray[PointersToThreadData[MinIndex]];
		CurrentLinkListPointer->Next=malloc(sizeof(LinkListNode_t));
		CurrentLinkListPointer = CurrentLinkListPointer->Next;
		PointersToThreadData[MinIndex]++;
		if(StartEnds[MinIndex][1]>=PointersToThreadData[MinIndex])
			buffer[MinIndex]=InputDataArray[PointersToThreadData[MinIndex]];
		else
			buffer[MinIndex]= MAX_NUMBER;
	}
}


clock_t begin,end;
void * mySPMDMain( void * arg )
{
	// Parameters
	int counter;
	int myId;
	struct ThreadControlBlock* myTCB;
	myTCB = (struct ThreadControlBlock *)arg;
	myId = myTCB->id;

	pthread_barrier_wait (&barrier);

	//for capturing start and end time
	//startTiming
	MASTER
	{
		gettimeofday(&start, NULL);
	}

	// Phase 1 :
	// Sorting Chunks of Data which is Related to the Process

	int StartIndex=0;
	int LocalSize = NUM_GLOBALINPUTDATA/NUM_THREADS;
	StartIndex = myId * LocalSize;
	int EndIndex=0;

	if(myId == NUM_THREADS-1)
		EndIndex = NUM_GLOBALINPUTDATA-1;
	else
		EndIndex = StartIndex + LocalSize -1;

	qsort(&GlobalInputData[StartIndex],EndIndex-StartIndex+1, sizeof(int), compare);

	// Doing the Regular Sampling
	FillRegularSample(RegularSample, GlobalInputData, myId, StartIndex, EndIndex);

	// End of Phase 1

	pthread_barrier_wait (&barrier);

	/*
	Phase 2
	Sorting Regular Sample and Extracting Pivots
	*/
	MASTER
	{
		qsort(RegularSample, NUM_THREADS*NUM_THREADS, sizeof(int), compare);
		ExtractPivots(Pivots, RegularSample);
	}
	// End of Phase 2

	pthread_barrier_wait (&barrier);

	/*
	Phase 3 :
	Partitioning the data and
	because we are doing Shared Memory Programming,
	the Exchanging part is excluded
	*/
	FillPartition(Pivots, GlobalInputData, myId, StartIndex, EndIndex);

	pthread_barrier_wait (&barrier);

	/*
	Phase 4
	Merging the Partitions by Merging Partial OrderedLists to a single LinkedList
	*/
	MergePartialOrderedLists(GlobalInputData, myId);

	pthread_barrier_wait (&barrier);

	//EndTiming();
	MASTER
	{
		gettimeofday(&stop, NULL);
		unsigned  long int   Duration= stop.tv_usec-start.tv_usec;
		printf("\n took : \n %ulli Mico sec , \n %lli Mili sec \n, %ulli Sec  to complete\n",Duration, Duration/1000,Duration/1000000);
	}
	return NULL;
} /*
mySPMDMain
*/
int main(void) {

	// Global Variables

	// Defining Input Data and Initializing it


	int counter;
	srandom(time(0));
	for( counter = 0; counter < NUM_GLOBALINPUTDATA; counter++ )
	{
			GlobalInputData[ counter ] = random() %MAX_NUMBER;
	}
	//printf("\n The Initial input is : \n ");
		//	  for(counter = 0; counter < NUM_GLOBALINPUTDATA; counter++)
			//    printf("%d ,", GlobalInputData[counter]);

	// Initializing Barrier Synch object
	pthread_barrier_init (&barrier, NULL,NUM_THREADS );
	/*

	Start threads
	*/

	for( counter = 1; counter < NUM_THREADS; counter++ )
	{
		TCB[ counter ].id = counter;     /*
		In parameter
		 */
		pthread_create( &( ThreadID[ counter ] ), NULL, mySPMDMain, (void*)&( TCB[ counter ] ) );
	}
	TCB[ 0 ].id = 0;
	mySPMDMain( (void*)&( TCB[ 0 ] ) );
	/*
	Clean up and exit
	*/
	for( counter = 1; counter < NUM_THREADS; counter++ )
		{
			TCB[ counter ].id = counter;     /*
			In parameter
			 */
			pthread_detach(ThreadID[counter]);

		}

	pthread_barrier_destroy(&barrier) ;
	return 0;
	return EXIT_SUCCESS;
}
