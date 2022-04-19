#!/usr/bin/env python
# coding: utf-8

# In[ ]:


pip install apache-beam[gcp]


# In[ ]:


pip install apache-beam[interactive]


# In[ ]:


pip install pandas


# In[ ]:


pip install gsutil


# In[15]:


#Total number of records: 2,175,000
#with open("1GB.txt",'w',encoding = 'utf-8') as f:
#    for i in range(0, 725000):
#        f.write('''{'col1': '125125', 'col2': 'asfsmalsafaf', 
#                'col3': '10.4', 'col4': 'asdmqldwgsd', 'col5': 'glkdsfkj', 
#                'col6': 'fdslfkmwfl', 'col7': 'dgklmwflkds', 
#                'col8': 'dfwlfdgkjwekl', 'col9': 'faslkjdlwdj', 'col10': '98908967'
#                'col11': 'cvmhlkqlkm', 'col12': 'dfjknenjkkjw', 'col13': 'wgpmcxclklksmdf', 
#                'col14': '11.3', 'col15': 'ffsdfefsfpowe', 'col16': 'dflkwmecvvbwe'
#                }''')
#        f.write('''{'col1': '5325125', 'col2': 'asghfdgalsafaf', 
#                'col3': '30.4', 'col4': 'asdmyjtjtyjwgsd', 'col5': 'gmnnlkkj', 
#                'col6': 'dsfklwfl', 'col7': 'duyjtfdhflkds', 
#                'col8': 'ddgkuykekl', 'col9': 'fadsvcxvwdj', 'col10': '98543967'
#                'col11': 'cvnbvcnqlkm', 'col12': 'dfjthrtehkkjw', 'col13': 'wgqwerlksmdf', 
#                'col14': '50.3', 'col15': 'ffjhgjffsfpowe', 'col16': 'dffwefcvvbwe'
#                }''')
#        f.write('''{'col1': '8765125', 'col2': 'astrusafaf', 
#                'col3': '16.9', 'col4': 'ascvxbdwgsd', 'col5': 'gqersfkj', 
#                'col6': 'fdsngfnwfl', 'col7': 'dghghtrflkds', 
#                'col8': 'dfwlfcvbkl', 'col9': 'fasqwrqwlwdj', 'col10': '9854667'
#                'col11': 'cthrlkqlkm', 'col12': 'dfjxvcbnjkkjw', 'col13': 'wglklksmdf', 
#                'col14': '19.3', 'col15': 'ffsdfpowe', 'col16': 'dflkwvvbwe'
#                }''')
#Total number of records: 4,350,000
#with open("2GB.txt",'w',encoding = 'utf-8') as f:
#    for i in range(0, 1450000):
#        f.write('''{'col1': '125125', 'col2': 'asfsmalsafaf', 
#                'col3': '10.4', 'col4': 'asdmqldwgsd', 'col5': 'glkdsfkj', 
#                'col6': 'fdslfkmwfl', 'col7': 'dgklmwflkds', 
#                'col8': 'dfwlfdgkjwekl', 'col9': 'faslkjdlwdj', 'col10': '98908967'
#                'col11': 'cvmhlkqlkm', 'col12': 'dfjknenjkkjw', 'col13': 'wgpmcxclklksmdf', 
#                'col14': '11.3', 'col15': 'ffsdfefsfpowe', 'col16': 'dflkwmecvvbwe'
#                }''')
#        f.write('''{'col1': '5325125', 'col2': 'asghfdgalsafaf', 
#                'col3': '30.4', 'col4': 'asdmyjtjtyjwgsd', 'col5': 'gmnnlkkj', 
#                'col6': 'dsfklwfl', 'col7': 'duyjtfdhflkds', 
#                'col8': 'ddgkuykekl', 'col9': 'fadsvcxvwdj', 'col10': '98543967'
#                'col11': 'cvnbvcnqlkm', 'col12': 'dfjthrtehkkjw', 'col13': 'wgqwerlksmdf', 
#                'col14': '50.3', 'col15': 'ffjhgjffsfpowe', 'col16': 'dffwefcvvbwe'
#                }''')
#        f.write('''{'col1': '8765125', 'col2': 'astrusafaf', 
#                'col3': '16.9', 'col4': 'ascvxbdwgsd', 'col5': 'gqersfkj', 
#                'col6': 'fdsngfnwfl', 'col7': 'dghghtrflkds', 
#                'col8': 'dfwlfcvbkl', 'col9': 'fasqwrqwlwdj', 'col10': '9854667'
#                'col11': 'cthrlkqlkm', 'col12': 'dfjxvcbnjkkjw', 'col13': 'wglklksmdf', 
#                'col14': '19.3', 'col15': 'ffsdfpowe', 'col16': 'dflkwvvbwe'
#                }''')
#Total number of records: 10,875,000
#with open("5GB.txt",'w',encoding = 'utf-8') as f:
#    for i in range(0, 3625000):
#        f.write('''{'col1': '125125', 'col2': 'asfsmalsafaf', 
#                'col3': '10.4', 'col4': 'asdmqldwgsd', 'col5': 'glkdsfkj', 
#                'col6': 'fdslfkmwfl', 'col7': 'dgklmwflkds', 
#                'col8': 'dfwlfdgkjwekl', 'col9': 'faslkjdlwdj', 'col10': '98908967'
#                'col11': 'cvmhlkqlkm', 'col12': 'dfjknenjkkjw', 'col13': 'wgpmcxclklksmdf', 
#                'col14': '11.3', 'col15': 'ffsdfefsfpowe', 'col16': 'dflkwmecvvbwe'
#                }''')
#        f.write('''{'col1': '5325125', 'col2': 'asghfdgalsafaf', 
#                'col3': '30.4', 'col4': 'asdmyjtjtyjwgsd', 'col5': 'gmnnlkkj', 
#                'col6': 'dsfklwfl', 'col7': 'duyjtfdhflkds', 
#                'col8': 'ddgkuykekl', 'col9': 'fadsvcxvwdj', 'col10': '98543967'
#                'col11': 'cvnbvcnqlkm', 'col12': 'dfjthrtehkkjw', 'col13': 'wgqwerlksmdf', 
#                'col14': '50.3', 'col15': 'ffjhgjffsfpowe', 'col16': 'dffwefcvvbwe'
#                }''')
#        f.write('''{'col1': '8765125', 'col2': 'astrusafaf', 
#                'col3': '16.9', 'col4': 'ascvxbdwgsd', 'col5': 'gqersfkj', 
#                'col6': 'fdsngfnwfl', 'col7': 'dghghtrflkds', 
#                'col8': 'dfwlfcvbkl', 'col9': 'fasqwrqwlwdj', 'col10': '9854667'
#                'col11': 'cthrlkqlkm', 'col12': 'dfjxvcbnjkkjw', 'col13': 'wglklksmdf', 
#                'col14': '19.3', 'col15': 'ffsdfpowe', 'col16': 'dflkwvvbwe'
#                }''')
#Total number of records: 21,750,000
#with open("10GB.txt",'w',encoding = 'utf-8') as f:
#    for i in range(0, 7250000):
#        f.write('''{'col1': '125125', 'col2': 'asfsmalsafaf', 
#                'col3': '10.4', 'col4': 'asdmqldwgsd', 'col5': 'glkdsfkj', 
#                'col6': 'fdslfkmwfl', 'col7': 'dgklmwflkds', 
#                'col8': 'dfwlfdgkjwekl', 'col9': 'faslkjdlwdj', 'col10': '98908967'
#                'col11': 'cvmhlkqlkm', 'col12': 'dfjknenjkkjw', 'col13': 'wgpmcxclklksmdf', 
#                'col14': '11.3', 'col15': 'ffsdfefsfpowe', 'col16': 'dflkwmecvvbwe'
#                }''')
#        f.write('''{'col1': '5325125', 'col2': 'asghfdgalsafaf', 
#                'col3': '30.4', 'col4': 'asdmyjtjtyjwgsd', 'col5': 'gmnnlkkj', 
#                'col6': 'dsfklwfl', 'col7': 'duyjtfdhflkds', 
#                'col8': 'ddgkuykekl', 'col9': 'fadsvcxvwdj', 'col10': '98543967'
#                'col11': 'cvnbvcnqlkm', 'col12': 'dfjthrtehkkjw', 'col13': 'wgqwerlksmdf', 
#                'col14': '50.3', 'col15': 'ffjhgjffsfpowe', 'col16': 'dffwefcvvbwe'
#                }''')
#        f.write('''{'col1': '8765125', 'col2': 'astrusafaf', 
#                'col3': '16.9', 'col4': 'ascvxbdwgsd', 'col5': 'gqersfkj', 
#                'col6': 'fdsngfnwfl', 'col7': 'dghghtrflkds', 
#                'col8': 'dfwlfcvbkl', 'col9': 'fasqwrqwlwdj', 'col10': '9854667'
#                'col11': 'cthrlkqlkm', 'col12': 'dfjxvcbnjkkjw', 'col13': 'wglklksmdf', 
#                'col14': '19.3', 'col15': 'ffsdfpowe', 'col16': 'dflkwvvbwe'
#                }''')


# In[ ]:





# In[ ]:




