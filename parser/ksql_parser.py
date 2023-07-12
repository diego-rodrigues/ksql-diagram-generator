
from parser.ksql_item import KSQLItem, KSQLTable, KSQLStream
from diagrams import Diagram, Edge, Node

# Diagram properties

node_attr = {
  "fixedsize": "false",
  "labelloc": "c",
  # "fillcolor": "magenta"
}

graph_attr = {
  "fontsize": "30",
  # "landscape": "false",
  "layout": "dot",
  "linelength": "10",
  # "splines": "line",
  "splines": "ortho",
  "bgcolor": "grey98",
  "ranksep": "2",
  "nodesep": "1",
}
    

class KSQLParser:
  def __init__(self):
    self.items = {}
    self.orderItems = []

  def _join_multilines(self, line_start, lines) -> str:
    out = lines[line_start] + " "
    line_start += 1
    curr_line = lines[line_start]
    max_lines = len(lines)
    while not(";" in curr_line or "EMIT" in curr_line) and line_start+1 < max_lines:
      out = out + curr_line.strip()
      line_start += 1
      curr_line = lines[line_start]
      
    return out

  def _parseStatement(self, input: str) -> KSQLItem:

    lines = input.split("\n")
    item = None
    line_no = 0

    for line in lines:
      lowerline = line.lower()

      # ignoring INSERT statements
      if "insert into" in lowerline:
        return None

      if " stream " in lowerline:
        item = KSQLStream(self._extract_name(lowerline, "stream"))
      
      elif " table " in lowerline:
        item = KSQLTable(self._extract_name(lowerline, "table"))
      
      elif (" key" in lowerline and "key_format" not in lowerline):
        item.withKey(self._extract_key(lowerline))
      
      elif ("group by" in lowerline):
        # treat the case where multiple keys are in the group by clause
        multiline = self._join_multilines(line_no, lines)
        item.withKey(self._extract_from_keyword(multiline.lower(), "group by ", False))
      
      elif ("partition by" in lowerline):
        multiline = self._join_multilines(line_no, lines)
        item.withKey(self._extract_from_keyword(multiline.lower(), "partition by ", False))
      
      elif "kafka_topic" in lowerline:
        item.withTopic(self._extract_topic(lowerline))

      elif "from " in lowerline:
        item.withOrigin(self._extract_from_keyword(lowerline, "from "))

      elif "inner join" in lowerline:
        item.withJoin(self._extract_from_keyword(lowerline, "inner join "),"INNER JOIN")
        item.withKey(self.get_item(item.origin).key)

      elif "left join" in lowerline:
        item.withJoin(self._extract_from_keyword(lowerline, "left join "),"LEFT JOIN")
        item.withKey(self.get_item(item.origin).key)

      line_no += 1
      
    return item


  def parseStatements(self, input: str):
    for statement in input.strip().split(";"):
      item = self._parseStatement(statement)
      
      if item != None:
        self.orderItems.append(item)
        self.items[item.name] = item


  def get_item(self, key: str):
    if key in self.items:
      return self.items[key]
    
    return None


  def print(self):
    for item in self.items:
      print(self.items[item])
      print("-=-=-=-=-=-=-")


  def _extract_name(self, input: str, type: str) -> str:
    linepart = input.split(" "+type+" ",2)[1]
    linepart = linepart.split("(")[0].strip()
    return linepart


  def _extract_key(self, input: str) -> str:
    linepart = input.strip().split(" ",2)[0].strip()
    return linepart


  def _extract_topic(self, input: str) -> str:
    linepart = input.strip().split("\'")[1]
    return linepart


  def _extract_from_keyword(self, input:str, keyword:str, with_alias:bool = True) -> str:
    if with_alias:
      linepart = input.strip().split(keyword)[1].split(" ")[0]
    else:
      linepart = input.strip().split(keyword)[1]
    return linepart


  def multilines(self, input: str, max_size:int =25) -> str:
    """
      Formats a string to multilines.
    """
    fact = 1
    out = input[0 : max_size]
    while len(input) > max_size * fact:
      out = out + "\n" + input[(fact)*max_size : (fact+1)*max_size]
      fact += 1
      
    return out
  
  def draw_diagram(self, input_file:str, diagram_name:str, output_name:str):

    f = open(input_file, "r")
    inputs = f.read()

    self.parseStatements(inputs)
    # p.print()

    items = self.items
    orderItems = self.orderItems

    draw_objects = {}
    topic_mappings = {}
    with Diagram(diagram_name, show=True, filename=output_name, direction="LR", \
                graph_attr=graph_attr):
      for item in orderItems:
      
        # tables
        if isinstance(item, KSQLTable):
        
          if (item.key == ""):
            draw_objects[item.name] = Node(label=self.multilines(item.name), height="0.6", \
                                      fixedsize="false", labelloc="c", shape="rectangle", \
                                      color="black", style="filled", fillcolor="peachpuff")
          else:
            draw_objects[item.name] = Node(label=self.multilines(item.name), xlabel=self.multilines(item.key,30), height="0.6", \
                                      fixedsize="false", labelloc="c", shape="rectangle", \
                                      color="black", style="filled", fillcolor="peachpuff")
            
        # streams
        elif isinstance(item, KSQLStream):
          draw_objects[item.name] = Node(label=self.multilines(item.name), xlabel=self.multilines(item.key), height="0.6", \
                                        fixedsize="false", labelloc="c", shape="box", \
                                        color="black", style="filled,rounded", fillcolor="lightskyblue")

        # arrows
        if item.origin != "":
            
          if "_rk" in item.name:
            draw_objects[item.origin] >> Edge(color="purple4", minlen="1", xlabel="RK") >> draw_objects[item.name]
          else:
            draw_objects[item.origin] >> Edge(color="black", minlen="1", style="tapered") >> draw_objects[item.name]


        # table-stream equivalents
        if item.topic in topic_mappings:
            
          for item2 in topic_mappings[item.topic]:
            draw_objects[item2] >> Edge(color="blue", minlen="1", style="dashed", arrowtail="none", arrowhead="none") >> draw_objects[item.name]
            
          topic_mappings[item.topic].append(item.name)
            
        else:
          topic_mappings[item.topic] = [item.name]


        # joins
        if len(item.joins) > 0:
          for joined_tuple in item.joins:
            joined = joined_tuple[0]
            jointype = joined_tuple[1]
            draw_objects[joined] >> Edge(color="firebrick", style="dashed", xlabel=jointype, minlen="1") >> draw_objects[item.name]
        
