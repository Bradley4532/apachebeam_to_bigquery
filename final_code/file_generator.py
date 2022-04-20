def generate_file(filename, numOfIters):
    with open(filename,'w',encoding = 'utf-8') as f:
        for i in range(1, numOfIters+1):
            if i == int(numOfIters/8) or i == int(4*numOfIters/8) or i == int(7*numOfIters/8) or i == int(8*numOfIters/8):
                f.write('''Brad{},{},hello{},8288884444\n'''.format(i,i,i))
            elif i == int(2*numOfIters/8) or i == int(5*numOfIters/8):
                f.write('''Brad{},{},,8288884444\n'''.format(i,i))
            elif i == int(3*numOfIters/8) or i == int(6*numOfIters/8):
                f.write('''Brad{},{},8.4,\n'''.format(i,i))
            else:
                f.write('''Brad{},{},10.4,8288884444\n'''.format(i,i))

if __name__ == "__main__":
    #Total number of records: 2,175,000
    generate_file("test_1GB.csv", 29514737)

    #Total number of records: 4,350,000
    #generate_file("test_2GB.csv", 59029474)

    #Total number of records: 10,875,000
    #generate_file("test_5GB.csv", 147573685)

    #Total number of records: 21,750,000
    #generate_file("test_10GB.csv", 295147370)