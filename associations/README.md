```
tr -c '[:alnum:]' '[\n*]' < retail.dat.txt | sort | uniq -c | sort -nr | head  -10
```