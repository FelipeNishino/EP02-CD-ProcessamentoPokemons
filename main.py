from mrjob.job import MRJob
from mrjob.step import MRStep

tipoPosicoes = {"Normal" : 8, "Fire" : 9, "Water" : 10, "Electric" : 11 , "Grass" : 12, "Ice" : 13, "Fighting" : 14, "Poison" : 15, "Ground" : 16, "Flying" : 17, "Psychic" : 18, "Bug" : 19, "Rock" : 20, "Ghost" : 21, "Dragon" : 22, "Dark" : 23, "Steel" : 24, "Fairy" : 25}
    
# python main.py novo.csv

class CalculaMelhorTime(MRJob):
  def steps(self):
    return [
        MRStep(mapper=self.separaPorTipo,
        reducer=self.juntaTipos),
        MRStep(mapper=self.CalculaDano,
        reducer=self.reducerFinal)
    ]

  def separaPorTipo(self, _, valor):
    dados = valor.rstrip("\n").split(',')
    tipo = dados[4]
    tipo += dados[5] if dados[5] != "Unknown" else ""
    yield tipo, { 'dados': dados }

  def juntaTipos(self, chave, valores):
    yield chave, valores

  def CalculaDano(self, chave, valores):
    todos = list(valores)
    pokemon = todos[0]["dados"]
    if pokemon[8] == "":
      i = 0
      while i < len(todos):
        if todos[i]["dados"][8] == '':
          i += 1
        else:
          pokemon = todos[i]["dados"]
          break

    fraqueza = []
    for tipo,pos in tipoPosicoes.items():
        mult = pokemon[pos][1:] 
        mult = mult if mult != "" else "1"
        fraqueza.append([tipo, float(mult)])

    fraqueza = filter(lambda x: (x[1] >= 2), fraqueza)
    fraqueza = sorted(fraqueza, key = lambda x: x[1], reverse=True )


    yield "", {'tipo': chave, 'fraqueza':fraqueza, 'valores': todos}


  def reducerFinal(self, chave, valores):
    chunks = {}
    for valor in valores:
      chunks[valor['tipo']] = valor

    for _, chunk in chunks.items():
      if len(chunk['fraqueza']) == 0:
        for poke in chunk['valores']:
          #pokemons.append({'pokemon': poke['dados'], 'counters': []})
          yield poke["dados"][0], "Nenhum counter encontrado"
      else:
        counters = []
        for fraq in chunk['fraqueza']:
          for counter in chunks[fraq[0]]['valores']:
            # if counter['dados'][7] == "Final":
            pk = counter['dados'][0:2]
            pk.append(counter['dados'][7])
            #counters.append(counter['dados'][0:2])
            counters.append(pk)
        counters = sorted(counters, key = lambda x:0 if x[2] == "Final" else 1)
        counters = list(map(lambda x: x[:2], counters))
        for poke in chunk['valores']:
          #pokemons.append({'pokemon': poke['dados'], 'counters': counters})
          yield poke["dados"][0], counters[0:10]

if __name__ == "__main__":
    CalculaMelhorTime.run()