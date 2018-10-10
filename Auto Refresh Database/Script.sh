#!/bin/bash
clear
SCRIPT_CURRENT_DIRECTORY="`dirname \"$0\"`"
date=`date +"%F"`
mkdir $SCRIPT_CURRENT_DIRECTORY/log/"$date"
echo "########### [`date +"%F-%T"`] ###########" > $SCRIPT_CURRENT_DIRECTORY/log/"$date"/AssignedValues
## Parse Database Names And Assign It To Array Variable 
db_name=`grep -w "Database" $SCRIPT_CURRENT_DIRECTORY/settings.conf | cut -f2 -d":"`

echo "[`date +"%T"`] Database Name : $db_name" >> $SCRIPT_CURRENT_DIRECTORY/log/"$date"/AssignedValues

number_of_tables=`tail -1 $SCRIPT_CURRENT_DIRECTORY/settings.conf | grep -F "Table" | cut -f2  -d":" | cut -f2 -d"."`

echo "[`date +"%T"`] Number Of Tables : $number_of_tables" >> $SCRIPT_CURRENT_DIRECTORY/log/"$date"/AssignedValues

for ((i=1;i<=$number_of_tables;i++))
do
    echo "########################################################" >> $SCRIPT_CURRENT_DIRECTORY/log/"$date"/AssignedValues
    table_name=`grep -F "Table" $SCRIPT_CURRENT_DIRECTORY/settings.conf | grep -F ".$i." | cut -f3 -d":"`
    echo "[`date +"%T"`] Table Name : $table_name" >> $SCRIPT_CURRENT_DIRECTORY/log/"$date"/AssignedValues

    impala-shell -q  "desc $table_name " > "$SCRIPT_CURRENT_DIRECTORY/log/"$date"/"$table_name-coulmns-bkp""  
    if [[ "$?" -ne "0" ]]
    then
        echo "Error"
    else
	cut -f2 -d"|" "$SCRIPT_CURRENT_DIRECTORY/log/"$date"/"$table_name-coulmns-bkp"" | grep -v "name" | grep -v "+--*" > "$SCRIPT_CURRENT_DIRECTORY/log/"$date"/"$table_name-coulmns""
        echo "[`date +"%T"`] Fields : " >> $SCRIPT_CURRENT_DIRECTORY/log/"$date"/AssignedValues
        cat "$SCRIPT_CURRENT_DIRECTORY/log/"$date"/"$table_name-coulmns"" >> $SCRIPT_CURRENT_DIRECTORY/log/"$date"/AssignedValues
        cat "$SCRIPT_CURRENT_DIRECTORY/log/"$date"/"$table_name-coulmns"" > $SCRIPT_CURRENT_DIRECTORY/log/"$date"/"ORG-$table_name-coulmns"
        number_of_fields=`grep -F "Table" $SCRIPT_CURRENT_DIRECTORY/settings.conf | grep -F "$table_name" | cut -f4 -d":"`
        echo "[`date +"%T"`] Number Of Fields : $number_of_fields" >> $SCRIPT_CURRENT_DIRECTORY/log/"$date"/AssignedValues
        for ((j=1;j<=$number_of_fields;j++))
        do
            eval $table_name"_field_"$j=`grep -F "Table" $SCRIPT_CURRENT_DIRECTORY/settings.conf | grep -F "$table_name" | cut -f5 -d":" | cut -f$j -d","`
            echo [`date +"%T"`] Field $j : $( eval "echo \$$table_name"_field_"$j" ) >> $SCRIPT_CURRENT_DIRECTORY/log/"$date"/AssignedValues
            OLD=$(eval "echo \$$table_name"_field_"$j")
            sed -i '/'$OLD'/d' "$SCRIPT_CURRENT_DIRECTORY/log/"$date"/"$table_name-coulmns""
            Selected_Fields[$j]=$OLD
            if [ "$j" -eq "1" ]
            then
                    Selected_Fields[0]+="${Selected_Fields[$j]}"
            else    
                    Selected_Fields[0]+=" ,${Selected_Fields[$j]}"
            fi
        done
        echo ${Selected_Fields[0]}
        
        echo
        ORG_Selected_Fields_num=`wc -l "$SCRIPT_CURRENT_DIRECTORY/log/"$date"/"$table_name-coulmns"" | cut -f1 -d" "`
        for ((j=1;j<="$ORG_Selected_Fields_num";j++))
        do
            file=$(eval "echo \$$SCRIPT_CURRENT_DIRECTORY/log/"$date"/"$table_name-coulmns"")
            ORG_Selected_Fields[$j]=`sed -n ''$j'p' "$SCRIPT_CURRENT_DIRECTORY/log/"$date"/"$table_name-coulmns""`
            echo ${ORG_Selected_Fields[$j]}
            if [ "$j" -eq "1" ]
            then
                    ORG_Selected_Fields[0]+="${ORG_Selected_Fields[$j]}"
            else    
                    ORG_Selected_Fields[0]+=" ,${ORG_Selected_Fields[$j]}"
            fi
        done
        echo ${ORG_Selected_Fields[0]}
        echo
    fi  
    impala-shell -q "use $db_name; drop table if exists "$table_name"_p; CREATE EXTERNAL TABLE "$table_name"_p partitioned by ( ${Selected_Fields[0]} ) STORED AS PARQUET AS SELECT ${ORG_Selected_Fields[0]} ,${Selected_Fields[0]} from $table_name"
    unset Selected_Fields[0]
    unset ORG_Selected_Fields[0]
done 
