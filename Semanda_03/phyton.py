a = float(input('Digite o valor do lado a:'))
b = float(input('Digite o valor do lado b:'))
c = float(input('Digite o valor do lado c:'))
 
if (a**2 == b**2+c**2 or b**2 == a**2 + c**2 or c**2 == a**2 + b**2):
 print('Os lados formam um trinângulo retângulo.')
elif (a**2 != b**2+c**2 or b**2 != a**2 + c**2 or c**2 != a**2 + b**2):
 print('Os lados não formam um triângulo retângulo.')
elif (c**2 == b**2+a**2 or b**2 == c**2 + a**2 or a**2 == c**2 + b**2):
  print ('Os lados não formam um triângulo.'