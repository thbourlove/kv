import kv

nothing = kv.Answer()

key = ""
for i in range(100):
    key = key + "f"

value = ""
for i in range (3000):
    value = value + "b"

print key, value

length = 900002

#for i in range(length):
    #nothing.put(key+"{}".format(i), value+"{}".format(i))

print nothing.get(key+str(1000))
print nothing.get(key+str(length - 1))
print nothing.get(key+str(length - 2))
