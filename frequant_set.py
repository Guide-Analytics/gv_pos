import csv
import nltk

#function that expands the frequant set table
def frequant_set_helper(sentence, set):
    temp = []
    i = 0
    #tag the sentence
    sentence = nltk.pos_tag(nltk.word_tokenize(sentence))
    #extract the tag set
    for i in range(len(sentence)):
        #add the tag into the set
        if (sentence[i])[1] != '.':
            temp.append((sentence[i])[1])
    #check if tag set already exist
    if len(set) == 0:
        set.append([temp,1])
    else:
        for i in range(len(set)):
            #if found a match in frequant set, increment the counter by 1, else insert into the set
            if (set[i])[0] == temp:
                (set[i])[1] += 1
                break
        #reached end of the list, and the set isnt in the frequant set
        if i == len(set) - 1 and (set[i])[0] != temp:
            set.append([temp,1])

#function takes a file of reviews and calls frequant_set_helper to build a set of items
def frequant_set_expand(file_name, set):
    f = open(file_name)
    for line in f:
        frequant_set_helper(line, set)

#helper function that merges 2 sorted list of tuples
def merge_help(set1,set2):
    result = []
    temp1 = len(set1)
    temp2 = len(set2)
    i1 = 0
    i2 = 0
    while(i1<temp1 and i2<temp2):
        if (set1[i1])[1] >= (set2[i2])[1]:
            result.append(set1[i1])
            i1+=1
        else:
            result.append(set2[i2])
            i2+=1
    if i1>=temp1 and i2 >=temp2:
        pass
    elif i1<temp1:
        while i1<temp1:
            result.append(set1[i1])
            i1+=1
    else:
        while i2<temp2:
            result.append(set2[i2])
            i2+=1
    return result

#function that sort the frequant set based on number of appearance
def sort_frequant_set(set):
    temp = len(set)
    if temp <= 1:
        return set
    else:
        first_half = sort_frequant_set(set[0:temp//2])
        second_half = sort_frequant_set(set[temp//2:temp])
        return merge_help(first_half,second_half)

#not used
#function that takes in a file of opinions and construct frequant set table, uses frequant_set_expand function
#def process_sentences(file_name):
#    f = open(file_name)
#    content = f.read()
#    list_of_sentences = nltk.sent_tokenize(content)
#    for element in list_of_sentences:
#        frequant_set_expand(element)
#    f.close()

#function that outputs the frequant set table to a text file
def output_frequant_set(file_name,set):
    #open file, create is not exist
    f = open(file_name,"w+")
    #go through the frequant_set
    for element in set:
        #for each entry, print out the set of grammars in "" seperated by spaces
        for grammar in element[0]:
            f.write("\"{}\" ".format(grammar))
        #print out the number of appearance, end with new_line
        f.write("{}\n".format(element[1]))
    f.close()

#not done yet
#def input_frequant_set(file_name):
#    f = open(file_name)