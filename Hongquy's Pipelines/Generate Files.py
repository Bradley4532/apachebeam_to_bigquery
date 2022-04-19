#!/usr/bin/env python
# coding: utf-8

# In[15]:


def generate_file(filename, numOfIters):
    with open(filename,'w',encoding = 'utf-8') as f:
        for i in range(0, numOfIters):
            f.write('''{'col1': '125125', 'col2': 'asfsmalsafaf', 
                    'col3': '10.4', 'col4': 'asdmqldwgsd', 'col5': 'glkdsfkj', 
                    'col6': 'fdslfkmwfl', 'col7': 'dgklmwflkds', 
                    'col8': 'dfwlfdgkjwekl', 'col9': 'faslkjdlwdj', 'col10': '98908967'
                    'col11': 'cvmhlkqlkm', 'col12': 'dfjknenjkkjw', 'col13': 'wgpmcxclklksmdf', 
                    'col14': '11.3', 'col15': 'ffsdfefsfpowe', 'col16': 'dflkwmecvvbwe'
                    }''')
            f.write('''{'col1': '5325125', 'col2': 'asghfdgalsafaf', 
                    'col3': '30.4', 'col4': 'asdmyjtjtyjwgsd', 'col5': 'gmnnlkkj', 
                    'col6': 'dsfklwfl', 'col7': 'duyjtfdhflkds', 
                    'col8': 'ddgkuykekl', 'col9': 'fadsvcxvwdj', 'col10': '98543967'
                    'col11': 'cvnbvcnqlkm', 'col12': 'dfjthrtehkkjw', 'col13': 'wgqwerlksmdf', 
                    'col14': '50.3', 'col15': 'ffjhgjffsfpowe', 'col16': 'dffwefcvvbwe'
                    }''')
            f.write('''{'col1': '8765125', 'col2': 'astrusafaf', 
                    'col3': '16.9', 'col4': 'ascvxbdwgsd', 'col5': 'gqersfkj', 
                    'col6': 'fdsngfnwfl', 'col7': 'dghghtrflkds', 
                    'col8': 'dfwlfcvbkl', 'col9': 'fasqwrqwlwdj', 'col10': '9854667'
                    'col11': 'cthrlkqlkm', 'col12': 'dfjxvcbnjkkjw', 'col13': 'wglklksmdf', 
                    'col14': '19.3', 'col15': 'ffsdfpowe', 'col16': 'dflkwvvbwe'
                    }''')
#Total number of records: 2,175,000
generate_file("1GB.txt", 725000)

#Total number of records: 4,350,000
generate_file("2GB.txt", 1450000)

#Total number of records: 10,875,000
generate_file("5GB.txt", 3625000)

#Total number of records: 21,750,000
generate_file("10GB.txt", 7250000)

