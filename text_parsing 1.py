arr_str=[]
for i in input_lines:
    arr_str.append(i.split(' ')) # a list of list of words
unlisted_arr=[val for sublist in arr for val in sublist] #flatten the list of list
characters = list(map(chr, range(97,123))) #map produces a set of response for the range and function chr
remove_non_words=[]
non_word=0
for i in unlisted_arr:
    for j in list(i):
        if j not in characters:
            non_word=1
            break
    if non_word==0:
        remove_non_words.append(i)
num_words=len(unlisted_arr)
output=str(num_words)+"\n"+"words"+"\n"
arr_str.sort()
words_num=[]
remove_duplicate=list(dict.fromkeys(unlisted_arr)) #removing duplicates in a list
for i in remove_duplicate:
    words_num.append(unlisted_arr.count(i))
    output=output+i+" "+str(words_num[len(words_num)-1])+"\n"
characters = list(map(chr, range(97,123)))
letters_num=[0]*len(characters) # a list of len(characters) of zeros
for i in unlisted_arr:
    for j in characters:
        if j in list(i):
            letters_num[characters.index(j)]=letters_num[characters.index(j)]+i.count(j)
output=output+"letters"+"\n"
for i in range(0, len(letters_num)):
    output=output+characters[i]+" "+str(letters_num[i])+"\n"
print(output)