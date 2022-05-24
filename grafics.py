# -*- coding: utf-8 -*-
"""
Created on Mon May 23 16:12:55 2022

@author: henry
"""

names = ["Joey Tribbiani", "Monica Geller", "Chandler Bing", "Phoebe Buffay"]
usernames = []

# write your for loop here
for i in names:
    usernames.append(i.lower().replace(" ","_"))
tokens = ['<greeting>', 'Hello World!', '</greeting>']
count = 0

# write your for loop here
for i in range(len(tokens)):
    if tokens[i][0] == "<":
        count += 1

print(count)


items = ['first string', 'second string']
html_str = "<ul>\n"  # "\ n" is the character that marks the end of the line, it does
                     # the characters that are after it in html_str are on the next line

# write your code here
for i in items:
    html_str = html_str+"<li>"+i+"</li>\n"
    

print(html_str)