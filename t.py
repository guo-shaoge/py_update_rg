test = {'a': 1, 'b': 2}
def func(test):
    test['a'] = 10

print('{}'.format(test))
func(test)
print('{}'.format(test))
