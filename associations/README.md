# Associations

For quickly extracting the most common 10 words i used the following shell command on the sample input file:
```
tr -c '[:alnum:]' '[\n*]' < retail.dat.txt | sort | uniq -c | sort -nr | head  -10
```