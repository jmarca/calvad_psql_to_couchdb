module.exports=function fix_component_names(direction,components){
    var patterns= {'wim':/wimid_(\d*)/
                  ,'vds':/(\d{5,7})/
                  }

    var dir;
    if(direction){ dir = direction.substr(0,1).toUpperCase() }
    return components.map(function(c){
               var result = patterns.wim.exec(c)
               if(result && result[1] !== undefined){
                   // wim detector
                   return ['wim',result[1],dir].join('.')
               }
               result = patterns.vds.exec(c)
               if(result && result[1] !== undefined){
                   // vds detector
                   return result[1]
               }
               return null
           })
}
