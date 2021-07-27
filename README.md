# Crisp Homework Assignment by Submitted Saqib Ali 7/26/2021


## Description:

## Pre-requisites: 
- Install [Docker](https://www.docker.com/)

## Instructions for running: 
- From the command line run: `docker-compose up crisp_homework`. 
That will run the program with the sample input file.

## Architectural overview
- I Dockerized this application so that it would run the same way regardless of your operating system 
and what software packages you have pre-installed

- I chose YAML for the configuration file because it is simple, well-understood and clean format. YAML parsers are readily available

- A key design decision was to completely decouple the inputter and outputter classes. They don't know anything about each other at all. 
Only the main program knows about both of them and directs data from the inputter to the outputter during the loop.

- Another important decision was to create two interfaces: `InputInterface` & `OutputInterface`. These interfaces define the contract that their sub-classes
must fulfill while remaining unaware of the lower-level details. This allows us to easily switch replace the input/output clases with different ones. 
Instead of reading from a file and writing to a file, we could easily implement small classes that will allow us to read from a database and write to a stream. 

- The configuration file contains keys named "Transform". The value associated with this key
is a python expression. I have used python syntax to piggy-back off so that the transform is easy to parse & subsequently evaluate
with the `eval()`. However `eval()` is an [insecure](https://realpython.com/python-eval-function/#minimizing-the-security-issues-of-eval) function. I used it 
instead of having to implement an entire grammar to specify the transforms. That would have been a
much bigger task. Furthermore, embedding python expressions into the configuration file means that any packages those expressions might need (like `datetime`)
need to be imported in the python code, even though that code never explicitly uses them. So it is a bit hacky.

- I used the [tqdm](https://github.com/tqdm/tqdm) package to provide me a fast, extensible and attractive progress-bar. Progress bars are a nice convenience 
for the user's who are operating long-running programs.
 
## Assumptions/Simplifications I made: 
- XXXX 

## Possible Next Steps:
- Add other input and output classes. 
For example reading and writing to a database seems like an obvious use case.
- Create a special-purpose grammar that can be used to define the transforms instead of python expressions that need to be
`eval()`ed.

