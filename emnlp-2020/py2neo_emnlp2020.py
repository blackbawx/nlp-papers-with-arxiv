from py2neo import Graph, Node, Relationship  # , authenticate
import os
import sys
#from add_py2neo import *
from collections import defaultdict
import csv

password = str(os.environ.get('NEO4J_PASSWORD'))
user = 'neo4j'

# authenticate("localhost:7474", user, password)
g = Graph("http://localhost:7474/db/data/", password=password)

posts_file = '/home/srallaba/projects/dissertation/emnlp2020.tdd'
papers_file = 'data/papers.csv'
nodestuff_file = 'nodes.emnlp2020'

tx = g.begin()
g.delete_all()

# Create a node for the conference
conference = Node("Conferences", name='EMNLP 2020')
tx.create(conference)
tx.commit()
tx = g.begin()

# Populate nodes
nodes_dict = {}
f = open(nodestuff_file)
for line in f:
  line = line.split('\n')[0].split(',')
  if len(line) < 2:
     continue
  key, val1, val2 = line[0], line[1], line[2]
  node = Node(val1, name=val2)
  nodes_dict[key] = node
  tx.create(node)
tx.commit()
tx = g.begin()

# Add papers
with open("data/papers.csv") as csvfile:
    f = csv.reader(csvfile)
    paper_list = [line for line in f]

for i, p in enumerate(paper_list[1:]):

    if i % 50 == 1:
       print("Added ", i, " papers" )

    p[1] = p[1].replace("\n", " ").replace("``", '"').replace("*", "\*").replace('-',' ')
    p[5] = p[5].replace('-',' ')

    assert p[4] == 'Accept' or p[4] == 'Accept-Findings'

    title = p[1].lower()
    abstract = p[5].lower()
    authors = p[2]

    paper = Node("Papers", name=title, summary=abstract)
    tx.create(paper)
    tx.commit()
    tx = g.begin()

    # Add to the conference
    relation = Relationship(conference, "HAS", paper)
    tx.create(relation)

    # Add the meta information to the paper node
    for k in nodes_dict.keys():
      if k in title or k in abstract:
         #print("Adding ",k)
         relation = Relationship(paper, "HAS", nodes_dict[k])
         tx.create(relation)

tx.commit()

