def high_avg(lines):
    line_lst=lines.split("\n")
    ip_lst=[]
    time_lst=[]
    for i in line_lst:
        temp_lst=i.split(" ")
        ip_lst.append(temp_lst[0])
        time_lst.append(temp_lst[len(temp_lst)-1])
    
    unique_ip=set(ip_lst)
    avg_lst=[]
    for i in unique_ip:
        indices=[index for index, value in enumerate(unique_ip) if value == i]
        avg_lst.append(sum(int(time_lst[i]) for i in indices)/len(indices))
    
    top_5_lst=sorted(range(len(avg_lst)), key=lambda i: avg_lst[i], reverse=True)[:5]
    return_lst=[]
    for i in top_5_lst:
        return_lst.append(list(unique_ip)[i])
    
    return return_lst