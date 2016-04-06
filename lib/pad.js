function pad1(width,char){
    if(char === undefined) char = '0'
    function pad(v){
        var i
        var padding = ''
        for(i = v.toString().length;
            i<width;
            i++){
            padding += char
        }
        return padding+v
    }
    return pad
}

module.exports=pad1
