from sys import argv, exit
import os

arguments = argv[1:]

if len(arguments) == 1:
    filename = arguments[0]
else: exit(1)

aux = filename.split(".")[0]
minute = aux[len(aux)-2:]

MINUTE_POSITION = 3
DELIMITER = ","
with open(filename, "r") as fin, open("temp.txt", "w") as temp:
    for line in fin:
        splited_line = line.split(",")

        splited_line[MINUTE_POSITION] = minute

        new_line = ""
        for i in range(len(splited_line)):
            new_line += splited_line[i]

            if i != len(splited_line) -1: new_line += DELIMITER
        
        temp.write(new_line)
    
os.remove(filename) # apaga o que estava com a formatacao antiga(errada)
os.rename(r'{}'.format(os.path.join("temp.txt")), r'{}'.format(os.path.join(filename)))