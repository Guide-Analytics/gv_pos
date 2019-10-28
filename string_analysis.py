from nltk import sent_tokenize
import nltk
from frequant_set import *
import re

# set of aspect words
aspect_table = []
# set of list [[] list of grammars,int number of appearance]
frequant_table = []
# set of list [[]list of grammars, []int]
noun_adj_rule_table = [[['JJ','JJ','JJ','JJ','JJ','NN'],[0,1,2,3,4,5,6]],[['JJ','JJ','JJ','JJ','NN'],[0,1,2,3,4,5]],
                       [['JJ','JJ','NN','OF','NN'],[0,1,4]],[['JJ','NN','OF','NN'],[0,3]],
                       [['JJ','NN','OF','NNS'],[0,3]],[['JJR','NN','OF','NN'],[0,3]],[['JJR','NN','OF','NNS'],[0,3]],
                       [['JJS','NN','OF','NN'],[0,3]],[['JJS','NN','OF','NNS'],[0,3]],
                       [['JJ','JJ','NN','IN','NN'],[0,1,2]],[['JJ','NN','IN','NN'],[0,1]],
                       [['JJ','NN','IN','NNS'],[0,1]],[['JJR','NN','IN','NN'],[0,1]],[['JJR','NN','IN','NNS'],[0,1]],
                       [['JJS','NN','IN','NN'],[0,1]],[['JJS','NN','IN','NNS'],[0,1]],[['JJ','NN','CC','NN'],[0,1,3]],
                       [['JJ','NN','CC','NNS'],[0,1,3]],[['JJ','NNS','CC','NNS'],[0,1,3]],
                       [['JJ','NNS','CC','NN'],[0,1,3]],[['JJR','NN','CC','NN'],[0,1,3]],
                       [['JJR','NN','CC','NNS'],[0,1,3]],[['JJR','NNS','CC','NNS'],[0,1,3]],
                       [['JJR','NNS','CC','NN'],[0,1,3]],[['JJS','NN','CC','NN'],[0,1,3]],
                       [['JJS','NN','CC','NNS'],[0,1,3]],[['JJS','NNS','CC','NNS'],[0,1,3]],
                       [['JJS','NNS','CC','NN'],[0,1,3]],[['JJ','JJ','JJ','NN'],[0,1,2,3]],
                       [['JJ','JJ','NN','NN'],[0,1,2,3]],[['NN','BE','RB'],[0,2]],[['NNS','BE','RB'],[0,2]],
                       [['NN','BE','RBR'],[0,2]],[['NNS','BE','RBR']],[['NN','BE','RBS'],[0,2]],
                       [['NNS','BE','RBS']],[['NN','BE','JJ'],[0,2]],[['NNS','BE','JJ'],[0,2]],
                       [['NN','BE','JJR'],[0,2]],[['NNS','BE','JJR']],[['NN','BE','JJS'],[0,2]],
                       [['NNS','BE','JJS']],[['NN','VBZ','JJ'],[0,2]],[['NNS','VBP','JJ'],[0,2]],
                       [['NN','VBD','JJ'],[0,2]],[['NNS','VBD','RBR'],[0,2]],[['NN','VBD','RBR'],[0,2]],
                       [['NN','VBZ','RBR'],[0,2]],[['NNS','VBP','RBR'],[0,2]],[['NNS','VBD','RBS'],[0,2]],
                       [['NN','VBD','RBS'],[0,2]],[['NN','VBZ','RBS'],[0,2]],[['NNS','VBP','RBS'],[0,2]],
                       [['NNS','VBD','JJR'],[0,2]],[['NN','VBD','JJR'],[0,2]],[['NN','VBZ','JJR'],[0,2]],
                       [['NNS','VBP','JJR'],[0,2]],[['NNS','VBD','JJ'],[0,2]],[['NNS','VBD','DT','JJS'],[0,2]],
                       [['NN','VBD','DT','JJS'],[0,2]],[['NN','VBZ','DT','JJS'],[0,2]],[['NNS','VBP','DT','JJS'],[0,2]],
                       [['JJ','NN','NN'],[0,1,2]],[['JJR','NN','NN'],[0,1,2]],[['JJS','NN','NN'],[0,1,2]],
                       [['JJ','JJ','NN'],[0,1,2]],[['JJ','NN'],[0,1]],[['JJR','NN'],[0,1]],[['JJS','NN'],[0,1]],
                       [['NN','NN'],[0,1]],[['NN','NNS'],[0,1]],[['JJ','NNS'],[0,1]],[['JJR','NNS'],[0,1]],
                       [['JJS','NNS'],[0,1]]]

# set of list [[]list of grammars, []int]
verb_rule_table = [[['NN','VBZ','RB'],[0,1]],[['NNS','VBP','RB'],[0,1]],[['NN','VBD','RB'],[0,1]],
                   [['NNS','VBD','RB'],[0,1]],[['NN','VBZ','RBR'],[0,1]],[['NNS','VBP','RBR'],[0,1]],
                   [['NN','VBD','RBR'],[0,1]],[['NNS','VBD','RBR'],[0,1]],[['NN','VBZ','DT','RBS'],[0,1]],
                   [['NNS','VBP','DT','RBS'],[0,1]],[['NN','VBD','DT','RBS'],[0,1]],[['NNS','VBD','DT','RBS'],[0,1]]]
# set of graph nod
graph_table = []


# function that takes 2 lists and reports a list of positions that contains graph nodes(noun, adjective),
# empty list if list 2 isn't a subpattern of list1
# input: list [POS's], list [[POS's],[int]]
def extract_position(lis1, lis2):
    i = 0
    j = 0
    # tracks the beginning of the pattern
    front = 0
    result = []
    if len(lis2[0]) > len(lis1):
        return result
    else:
        while i < len(lis1) and j < len(lis2[0]):
            if lis1[i] == (lis2[0])[j]:
                i += 1
                j += 1
            else:
                i += 1
                front = i
                j = 0
        if j < len(lis2[0]):
            return result
        else:
            # go through the position of noun, adjective) pairs in list
            for element in lis2[1]:
                # increment the dictionary by front to get the position at the actual sentence
                result.append(element + front)
            return result


# helper function that returns the position of root node given a word and graph set
# input:string word, graph_table
def pos_node(word,set):
    i = 0
    # iterate through the list and return position of node is found a match
    while i < len(set):
        if word == (set[i])[0]:
            return i
        i += 1
    return -1


# helper function for build_graph_helper, takes set of related words, expands the graph set
# input:list [words], list[list[word,list[words]]]
def graph_expand(word_set,graph_set):
    i = 0
    j = 0
    while i < len(word_set):
        # see if the current word is a root in the graph set, returns the position of the root if so, -1 otherwise
        temp = pos_node(word_set[i], graph_set)
        # case root node hasn't been created
        if temp == -1:
            child_insert = []
            # go through the list of words
            while j < len(word_set):
                # look for the words that are not the same as current and not in current child set, add to child set
                if (not (word_set[j] in child_insert)) and word_set[j] != word_set[i]:
                    child_insert.append((word_set[j], 1))
                j += 1
            # resets j
            j = 0
            # add the constructed new node with its children set onto the graph set
            graph_set.append([word_set[i],child_insert])
        # the root node already exists, see if any child node can be added
        else:
            # iterate through the whole list
            while j < len(word_set):
                # if the word isn't root itself
                if (word_set[j] != word_set[i]):
                    # dummy variable that iterates through the child list
                    dummy = 0
                    while dummy < len(graph_set[temp][1]):
                        # if the word matches the key in child list
                        if ((graph_set[temp][1])[dummy])[0] == word_set[j]:
                            # increment the counter of the child entry
                            (graph_set[temp][1])[dummy] = (word_set[j],(graph_set[temp][1][dummy][1])+1)
                            # found the entry, get out of the while loop
                            break
                        dummy += 1
                    # if dummy has iterated through the whole child list without finding a matching key, insert
                    if dummy == len(graph_set[temp][1]):
                        ((graph_set[temp])[1]).append((word_set[j],1))
                j += 1
            # reset j
            j = 0
        i += 1


# function that sorts the child set of graph_table
def sort_children(graph_set):
    for element in graph_set:
        element[1] = sort_frequant_set(element[1])


# function that pickles 'be' verbs for NLTK tagged list: resolves NN VBZ RB/NN VBZ JJ issues involving the be-verbs
def pickle_be(tagged_list):
    i = 0
    while i < len(tagged_list)-1:
        if (((tagged_list[i])[0] == 'is' and (tagged_list[i][1]) == 'VBZ') or
            ((tagged_list[i])[0] == 'are' and (tagged_list[i][1]) == 'VBP') or
            (((tagged_list[i])[0] == 'was' or (tagged_list[i])[1] == 'were') and
             (tagged_list[i][1]) == 'VBD')) and ((tagged_list[i+1])[1] == 'RB' or
                                                 (tagged_list[i+1])[1] == 'RBR' or (tagged_list[i+1])[1] == 'RBS'):
            tagged_list[i] = ((tagged_list[i])[0],'BE')
        i += 1
    return tagged_list


# function that pickles 'of' case for an NLTK tagged list: resolves JJ NN IN NN issues involving the word of
def pickle_of(tagged_list):
    i = 0
    while i < len(tagged_list):
        if((tagged_list[i])[0] == 'of' and (tagged_list[i])[1] == 'IN') or \
                ((tagged_list[i])[0] == 'oF' and (tagged_list[i])[1] == 'IN') or \
                ((tagged_list[i])[0] == 'Of' and (tagged_list[i])[1] == 'IN'):
            (tagged_list[i]) = ('of','OF')
        i += 1
    return tagged_list


# function that processes RB when it is used to describe the magnitude of Adjective
def process_rb(tagged_list):
    i = 0
    # list that keeps record of rb locations
    rb_pos_list = []
    # list that we need to remove the items from
    rm_list = []
    resulting_word = ""
    # first pass, locate all the rbs, adjs
    while i < len(tagged_list):
        if (tagged_list[i])[1] == 'RB' or (tagged_list[i])[1] == 'JJ' or (tagged_list[i])[1] == 'JJR' or \
                (tagged_list[i])[1] == 'JJS' or (tagged_list[i])[1] == 'RBR'or (tagged_list[i])[1] == 'RBS':
            rb_pos_list.append(i)
            rm_list.insert(0, i)
        i += 1
    i = 0
    while i < len(rb_pos_list)-1:
        # flag value indicating if there is remove process happening
        rm = False
        while i < len(rb_pos_list)-1 and rb_pos_list[i]+1 == rb_pos_list[i+1]:
            if not rm:
                resulting_word += (tagged_list[(rb_pos_list[i])])[0]
            else:
                resulting_word += "-"
                resulting_word += (tagged_list[(rb_pos_list[i])])[0]
            i += 1
            rm = True
        if rm:
            resulting_word += "-"
            resulting_word += (tagged_list[(rb_pos_list[i])])[0]
            tagged_list[(rb_pos_list[i])] = (resulting_word,(tagged_list[(rb_pos_list[i])])[1])
            rm_list.remove(rb_pos_list[i])
        i += 1
    for element in rm_list:
        del tagged_list[element]
    return tagged_list


# function that marks client name that is properly written i.e. canadian tire(with space),laura's family co(with spaces)
# input:string sentence, string client_name
def mark_client_proper(sentence,client_name):
    length = len(client_name)
    result = ""
    i = 0
    pattern = re.compile(client_name)
    it = pattern.finditer(sentence)
    for element in it:
        while i < len(sentence):
            if i >= element.start() and i < (element.start() + length) and sentence[i] == ' ':
                result += '-'
            else:
                result += sentence[i]
            i += 1
        sentence = result
        result = ""
        i = 0
    return sentence


#  function that marks client strictly as a noun
def pickle_client(tagged_list,client_name):
    i = 0
    while i < len(tagged_list):
        if (tagged_list[i])[0] == client_name and (((tagged_list[i])[1] != 'NN') or (((tagged_list[i])[1]) != 'NNS')):
            tagged_list[i] = (client_name, 'NN')
        i += 1
    return tagged_list


# function that helps build_graph by analysing a sentence and construct the graph accordingly
def build_graph_helper(sentence, client_name, frequant_set, graph_set):
    tag_list = []
    word_set = []
    # fixes the index as when we remove things from the list, size of the list changes
    index_fix = 0
    proper_client_name = ""
    for element in client_name:
        if element == ' ':
            proper_client_name += '-'
        else:
            proper_client_name += element
    # patches the client names together i.e. canadian tire -> canadian-tire
    sentence = mark_client_proper(sentence, client_name)
    sentence = nltk.pos_tag(nltk.word_tokenize(sentence))
    # pickle of tags
    sentence = process_rb(sentence)
    sentence = pickle_client(sentence,proper_client_name)
    sentence = pickle_of(sentence)
    sentence = pickle_be(sentence)
    # extract the tag list of sentence
    for element in sentence:
        tag_list.append(element[1])
    # go through the frequant_set list
    for element in frequant_set:
        # check if the structure of sentence has the signature of descriptive speeches in frequant set,
        # and get the positions of the words in the sentence
        temp = extract_position(tag_list, element)
        # print("tag list:{}, position:{}, temp is {}".format(tag_list,element, temp))
        # the sentence has more than 2 items that we want to extract
        if len(temp) > 1:
            for element in temp:
                word_set.append(sentence[element][0])
            # remove the corresponding entries from the sentence list after recording the words so we can check if there
            # are left over opinions to be extracted from the sentence
            for element in temp:
                del sentence[element-index_fix]
                index_fix += 1
            # expand the graph using new list of words
            graph_expand(word_set, graph_set)
            # reassemble the list of elements back into a string sentence
            new_sentence = ""
            for element in sentence:
                new_sentence += element[0]
                new_sentence += " "
            # run the function on leftover sentence to see if more opinions can be extracted
            build_graph_helper(new_sentence, client_name, frequant_set, graph_set)
            break
        # the temp has only 1 element in it which should never happen if frequant set is well designed
        elif len(temp) > 0:
            print("think we've ran into some problem, check carefully.\n")


# function that constructs the graph for analysis
# input:string file_name, list[list[word, list[words]]]
def build_graph(file, client_name, rule_set, graph_set):
    #f = open(file_name,encoding='utf-8')
    #file = f.readlines()
    for line in file:
        result = sent_tokenize(line)
        for sentence in result:
            build_graph_helper(sentence, client_name, rule_set, graph_set)
    sort_children(graph_set)

def build_graph_rdd(review, client_name, rule_set, graph_set):
    #f = open(file_name,encoding='utf-8')
    #file = f.readlines()
    for line in review:
        result = sent_tokenize(line)
        for sentence in result:
            build_graph_helper(sentence, client_name, rule_set, graph_set)
    sort_children(graph_set)


# function that prints graph_table into a text file
def print_graph(file_name, set):
    f = open(file_name,"w+",encoding='utf-8')
    for element in set:
        f.write("{}, ".format(element[0]))
        i = 0
        while i<len(element[1])-1:
            j = 0
            while j < element[1][i][1]:
                f.write("{}, ".format((element[1])[i][0]))
                j += 1
            i += 1
        f.write("{}\n".format((element[1])[i][0]))

#print_graph('output.txt', graph_table)