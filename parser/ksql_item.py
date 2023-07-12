class KSQLItem:
  def __init__(self, name):
    self.name = name
    self.key = ""
    self.topic = ""
    self.origin = ""
    self.joins = []

  def withKey(self, key: str):
    self.key = key

  def withTopic(self, topic: str):
    self.topic = topic

  def withOrigin(self, origin):
    self.origin = origin

  def withJoin(self, item, jointype):
    self.joins.append((item,jointype))


class KSQLTable(KSQLItem):
  def __str__(self):
    return f"TABLE {self.name}\n\t[{self.key}]\n\t[{self.topic}]\n\t[{self.origin}]\n\t{self.joins}"
    

class KSQLStream(KSQLItem):
  def __str__(self):
    return f"STREAM {self.name}\n\t[{self.key}]\n\t[{self.topic}]\n\t[{self.origin}]\n\t{self.joins}"
    
