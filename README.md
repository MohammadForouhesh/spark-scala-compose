# spark-scala

## results

### with stopwords included
![image](https://user-images.githubusercontent.com/17898264/202871996-97614470-a5bf-4916-a416-20140dec806c.png)


### with stopwords removed
![image](https://user-images.githubusercontent.com/17898264/202871975-d6f7dfd5-fd45-4429-9577-0f00e3fac9c1.png)


### A note on HDFS and Hadoop on windows:

It is silly to use windows in the first place, but you happen to be in such a dire situation, take the following path:
1- download winutils from https://github.com/kontext-tech/winutils
2- extract and put the \bin in C:\\hadoop
3- Add C:\\hadoop\bin to your environmental variable as HADOOP_HOME
4- copy hdfs.dll and hadoop.dll from C:\\hadoop\bin to C:\\Windows\System32

and you are done!
