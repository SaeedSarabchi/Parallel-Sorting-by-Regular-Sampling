#!/bin/bash
rm output7_8.txt

      for j in `seq 1 2`;
      do
 
                        if [ $j -eq "1" ]; then
                                num="16000000";
                        else
                                num="160000000";
                        fi

			for i in 16 32 64
                            do	

				echo "*******" "ShorooE " $i,$num"******" >> output7_8.txt
				for k in `seq 1 7`;
				do
					mpirun -np $i -f /home/ubuntu/hosts  /home/ubuntu/test/MPI_PSRS $num >> output7_8.txt
				done
				echo "******""Payane" $i,$num"********"  >> output7_8.txt
			done
        done  
