"""Geohash estandar, sin dependencias externas."""
ALPHABET='0123456789bcdefghjkmnpqrstuvwxyz'
def geohash(lat,lon,precision=5):
    ranges=[[-180.,180.],[-90.,90.]];axis=0;value=0;bits=0;result=''
    while len(result)<precision:
        bounds=ranges[axis];mid=sum(bounds)/2;coordinate=lon if axis==0 else lat
        high=coordinate>=mid;value=(value<<1)|int(high);bounds[0 if high else 1]=mid
        axis=1-axis;bits+=1
        if bits==5: result+=ALPHABET[value];value=0;bits=0
    return result

def geometry(code):
    ranges=[[-180.,180.],[-90.,90.]];axis=0
    for char in code:
        value=ALPHABET.index(char)
        for shift in range(4,-1,-1):
            bounds=ranges[axis];bounds[0 if (value>>shift)&1 else 1]=sum(bounds)/2;axis=1-axis
    (west,east),(south,north)=ranges
    return {'type':'Polygon','coordinates':[[[west,south],[east,south],[east,north],[west,north],[west,south]]]}
