from mainPipeline import Pipeline
import itertools
import csv
import os
pipeline=Pipeline()

@pipeline.task()

#This read in line from the text file and removes \n
def read_file(log):
            
    lines=[]
    
    with log as f:
        
        for line in f:       
            lines.append(line.strip("\n"))

    return lines
            
@pipeline.task(depends_on=read_file)


# This step separates each records. A new record is identified by a blank line.
def separate_records(lines):
    records=[]
    row=[]
    #print(lines)
    for line in lines:
        
        if line=='':
              
            records.append(row)
            row=[]
        else:   
            row.append(line)        
              
    return(records)

@pipeline.task(depends_on=separate_records)
 
# It extracts required fields       
def extract_fields(records):

    #created a common functionality to get value from each of these fields.
    def parse_field(field, record):
        value=''
        
        for elem in record:
            if field in elem:
                value=elem
            elif (len(value)!=0 and ':' not in elem):
                 value = value + ' '+ elem
            elif(len(value)!=0 and ':' in elem):
                break
            
        return value.replace(field,'')
     
    #date
    tempdate=0
    
    #Parsing through records..
    for record in records:
        
        if record[0].isupper():
            DATE=record[0]
            tempdate=DATE
        else:
            DATE=tempdate
        
        #calling parse_filed to get value from each record
        MTG=parse_field('MTG:', record)
        BANK=parse_field('BANK:', record)
        BORROWER=parse_field('BORROWER:', record)
        ADD=parse_field('ADD:', record)
        
        yield (DATE,MTG,BANK,BORROWER,ADD)
  
@pipeline.task(depends_on=extract_fields) 

def list_to_csv(lines):
     
    tempFile =open('ExtractedFields_file.csv','wb')
    def build_csv(lines, header=None, file=None):
         
        if header:
            lines = itertools.chain([header], lines)
        writer = csv.writer(file, delimiter=',')
        writer.writerows(lines)
        file.seek(0)
        return file
     
    return build_csv(lines, 
                     header=['DATE','MTG','BANK','BORROWER','ADD'], 
                     file=tempFile)


log=open ('data.txt')

split_lines=pipeline.run(log)
print(split_lines)