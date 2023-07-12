
import sys
from parser.ksql_parser import KSQLParser

if __name__ == "__main__":

  args = len(sys.argv) - 1
  if (args < 3):
    print("Usage: python ksql_diagram.py [input-ksql-file] [diagram-title] [output-name]")
    sys.exit()

  p = KSQLParser()
  p.draw_diagram(sys.argv[1],sys.argv[2],sys.argv[3])