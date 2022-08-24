
import sys
import json
import findspark
findspark.init()
import pyspark
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import *
from collections import Counter
from math import sqrt



if __name__ == "__main__":
    if len(sys.argv) != 1:
        print("Usar solo diccionatio como argumento", file=sys.stderr)
        int_dic=json.loads(sys.argv[1])
        print(int_dic.keys())
          
        spark = SparkSession\
                .builder\
                .appName("Algoritmo de precision")\
                .getOrCreate()
                
        #path_location= "/home/stecsdi/spark_db/database/"
        path_location= "/media/henryx/alldata/algoritmo_pre/datos/final/"
        
        data_pi_nam = int_dic['databapiv']
        opcionborrar = int_dic['opborrar']
        variables_into = int_dic['listaint']
        
        reg_pobl =  spark.read.format("csv")\
              .option("header", "true")\
              .load(path_location+'tem_reg_pob.csv')
         
        data_piv =  spark.read.format("csv")\
              .option("header", "true")\
              .option("index", "false")\
              .load(path_location+data_pi_nam)
        variables_into = ['p_cedula','p_nombres_apellidos','p_nombres','p_cedula_mad','p_apellidos','p_sexo','p_fecha_nac' ]

        

        
        #parametros que se pueden añadir
        
        Id_reg = 'p_idr'
        Id_piv = 'p_idp'
        ##  spark.sparkContext.setCheckpointDir
        # permite almacenar los datos salientes del proceso deterministico
        # diminuyendo el tiempo de pocesamiento 
        
        spark.sparkContext.setCheckpointDir(path_location+'cheakpoint/')
       # saprk.conf.set()
       # spark.conf.get("spark.cleaner.referenceTracking.cleanCheckpoints", "true")
       
        variables_de_tiempo=  {'p_fecha_nac', 'p_fecha_de_fatte'}
        
        variables_reg = set( reg_pobl.columns)
       
        
        """  Area de las funciones"""
        
        
        
        #verificamos si realmente las varibles ingresadas existes en la base pivot.
        variables_int1= []
        for i in variables_into:
            if  i in data_piv.columns:
                variables_int1.append(i)
        
        
        
        
        # ya con las variables obtenidas realizamos un mach con las variables del registro
        
        variables_int= []
        for i in variables_int1:
            if  i in variables_reg:
                variables_int.append(i)
             
        
        """ Agregamos identificadores a los registros target y pivot  """
        
        
        reg_pobl = reg_pobl.withColumn('p_idr',reg_pobl.p_cedula) #Se escoge la cédula como identificador del target
         
        
        
        wa = Window().orderBy(F.lit('A')) #variable sin importancias
        # para le registro pivot se escojen numeros segun su indice.
        data_piv = data_piv.withColumn(Id_piv, F.row_number().over(wa))
        data_piv = data_piv.withColumn(Id_piv, F.col(Id_piv).cast(StringType() ))
        data_piv = data_piv.select([Id_piv]+variables_int) 
        
        
        """agregamos nombres y apellidos juntos en caso de se necesario"""
        # F.udf crea una  funcion similar a map en la libreria pandas
        @F.udf 
        def join_apelidos( nom_ape, nom_ape_pi):
            if nom_ape:
                return nom_ape
            else:
             return nom_ape
         
        if  ('p_nombres' in data_piv.columns) and ('p_apellidos' in data_piv.columns): 
               data_piv=data_piv.select('*',F.concat_ws(' ',\
                           data_piv.p_apellidos, data_piv.p_nombres ).alias('na_ap_pivot'))
            
               if 'p_nombres_apellidos' in  data_piv.columns:
                 data_piv = data_piv.withColumn('p_nombres_apellidos', \
                    join_apelidos(data_piv.p_nombres_apellidos ,data_piv.na_ap_pivot))
                 data_piv =data_piv.drop('na_ap_pivot' )
               else:
                   data_piv=data_piv.withColumnRenamed('na_ap_pivot', 'p_nombres_apellidos')
        else:
                print("No existen nombres o apellidos")
               
        
        
        
        
        """ Dar iniciales registro no tiene iniciales """
        
        @F.udf
        def dar_iniciales(fullname):
            if fullname:
              name_list = fullname.split()
              initials = ""
              for name in name_list:  # va por cada elemento en grupo.
                 initials += name[0].upper()  # agrega inicial
            
              return initials
            else:
                fullname
            
        if "iniciales" in data_piv.columns:
             None
        else:
                 data_piv= data_piv.withColumn('iniciales', \
                     dar_iniciales(data_piv.p_nombres_apellidos))
           
                
        """ Definiendo variables finales"""
        
        
        
        dic_var_deter = { "id_cedula_nombre_apell":       ['p_cedula','p_nombres','p_apellidos'],\
                          "id_cedula_nombreyapllido":     ['p_cedula','p_nombres_apellidos'],\
                           "id_cedula_sexo_fna":          ['p_cedula','p_sexo','p_fecha_nac'],\
                          "id_nombre_apell_sexo_fnac":    ['p_nombres', 'p_apellidos','p_sexo','p_fecha_nac'],\
                          "id_nombreyapell_sexo_fnac":    ['p_nombres_apellidos','p_sexo','p_fecha_nac'],\
                          "id_cedula_iniciales_sexo":     ['p_cedula','p_iniciales','p_sexo'],\
    
                          "id_cedulma_fna_sexo":          ['p_cedula_mad','p_fecha_nac','p_sexo'],\
                          "id_cedulpa_fna_sexo":          ['p_cedula_pad','p_fecha_nac','p_sexo'],\
                          "id_cedulma_nombre_apell":      ['p_cedula_mad', 'p_nombres', 'p_apellidos'],\
                          "id_cedulma_nombreyapell":      ['p_cedula_mad', 'p_nombres_apellidos'],\
                          "id_cedulpa_nombre_apell":      ['p_cedula_pad', 'p_nombres', 'p_apellidos'],\
                          "id_cedulpa_nombreyapell":      ['p_cedula_pad', 'p_nombres_apellidos'],\
                          "id_cedulma_iniciale_sexo":     ['p_cedula_mad','iniciales','p_sexo'],\
                          "id_cedulpa_iniciale_sexo":     ['p_cedula_pad','iniciales','p_sexo'] 
                          }   
        
            
            
            
            
            
        
            
        dic_var_difu = { "idif_sexo_fna_cedulma_N_A":       [['p_sexo','p_fecha_nac'], \
                                ['p_nombres', 'p_apellidos','p_nombres_apellidos'],0],\
                        
                        "idif_sexo_fna_cedulma_N_A_30":     [['p_sexo','p_fecha_nac'], \
                                ['p_nombres', 'p_apellidos','p_nombres_apellidos'],30]}                
                       
                      
            
        """ceacion de dicionario a partir de las variables obtenidas"""
        
        
        def f_var_det_convi(dic_var_dete,dic_var_difu, variables_int):
            if variables_int:
                X= set(variables_int)  
                Y = {}
                Z = {}
                for i in  dic_var_dete.keys():
                   if  set(dic_var_dete[i]).issubset(X)  :
                       Y[i]= dic_var_dete[i] 
                
                for i in  dic_var_difu.keys():
                   if  set(dic_var_difu[i][0]+dic_var_difu[i][0] ).issubset(X)  :
                       Z[i]= dic_var_difu[i] 
                
                return  Y, Z
            else:
                print('Variables no definidas')
        
            
         
        
        dic_var_det_convi, dic_var_dif_convi = f_var_det_convi(dic_var_deter,dic_var_difu, data_piv.columns  )
            
           
        
        
        """ALGORITMO DEL COSENO"""
        
        def split_fun(word):
            # permite crear los n-grmas de los conjuntos
            li=[]
            for i in range(len(word)-1):
                li.append(word[i]+word[i+1] )
            return li
        def word2vec(word):
            #cuenta los caracteres de una una palabra
            cw = Counter(word)
            # trnasdorma en un conjunto de carateres
            sw = set(cw)
            # precomputes the "length" of the word vector
            lw = sqrt(sum(c*c for c in cw.values()))
        
            # return a tuple
            return cw, sw, lw
        
        
        def cosdis(v1, v2):
            if v1 and v2:
                v1=word2vec(split_fun(v1))
                v2=word2vec(split_fun(v2))
                # which characters are common to the two words?
                common = v1[1].intersection(v2[1])
                # by definition of cosine distance we have
                return  1-abs(sum(v1[0][i]*v2[0][i] for i in common)/v1[2]/v2[2])
            else: 
                return 1
        
        g_funt = F.udf(cosdis, FloatType())
        
        
        
        
        
        """coseno2"""
        def codisa(x,Y):
            X_set = set(X)
            Y_set = set(Y)
            rvector = X_set.union(Y_set) 
            for w in rvector:
                if w in X_set: l1.append(1) # create a vector
                else: l1.append(0)
                if w in Y_set: l2.append(1)
                else: l2.append(0)
            c = 0
              
            # cosine formula 
            for i in range(len(rvector)):
                    c+= l1[i]*l2[i]
            cosine = c / float((sum(l1)*sum(l2))**0.5)
            return cosine
        
        
        """ la funciones complementarias """
        # la funcion unico cuenta sobre las particiones
        # y elimina aquellos valores que se repiten dos veces
        #  en la variable "p_idr" variable del registro
        def unico(dat_tjo):
            return dat_tjo.selectExpr(
              '*', 
              'count(*) over (partition by p_idr) as cnt'
            ).filter(F.col('cnt') == 1).drop('cnt')
        
           
        # la funcion tim_df, toma como entrada un data frame
        # y devuelve un  dataframe con las
        # variables de fecha tipo to_timestamp()
        # cuyas variables_de_tiempo deben estar definidas
        # previamente. 
        def tim_df( df ):
             for i in variables_de_tiempo:
                 if i in df.columns:
                   df=  df.withColumn(i,F.to_timestamp(F.col(i)))
             return df   
        

        
        """ definimos el dataframe join"""
        # El dataframe contine las mismas variables definidas del 
        # pivot mas el identificador de registro
        #  ..p_idr..[variables del pivot]..id_etiqueta..
        # el id_etiqueta almacena las llaves de los metodos
         
        
        reg_jo = spark.createDataFrame([], data_piv.schema)
        reg_jo = reg_jo.withColumn('p_idr', F.lit(None).cast(StringType()))
        reg_jo = reg_jo.withColumn('id_etiqueta', F.lit(None).cast(StringType()))
        reg_jo = reg_jo.select(['p_idr']+data_piv.columns+['id_etiqueta'])
        
        
        
        #relizamos un cambio de variable
        re_piv = data_piv
        re_total = reg_pobl
        new_jo= reg_jo
        
        #axiliar 
        
        for key in dic_var_det_convi.keys(): 
                    print(key)
                    #key= "id_cedula_nombre_apell"
                    li= dic_var_det_convi[key]
                
                    if key== "id_cedulma_iniciale_sexo":
                        re_auxiliar= re_total.selectExpr('*', 'count(*) over (partition \
                                by p_cedula_mad,iniciales, p_sexo ) as cnt').filter(F.col('cnt') >1 ).drop('cnt') 
                        re_total= re_total.selectExpr('*', 'count(*) over (partition \
                                by p_cedula_mad,iniciales, p_sexo ) as cnt').filter(F.col('cnt') == 1).drop('cnt') 
                    elif  key=="id_cedulpa_iniciale_sexo":
                        re_auxiliar= re_total.selectExpr('*', 'count(*) over (partition \
                                by p_cedula_pad,iniciales, p_sexo ) as cnt').filter(F.col('cnt') >1 ).drop('cnt') 
                        re_total= re_total.selectExpr('*', 'count(*) over (partition \
                                by p_cedula_pad, iniciales, p_sexo ) as cnt').filter(F.col('cnt') == 1).drop('cnt') 
                    elif  key=="id_cedulma_fna_sexo":
                        re_auxiliar= re_total.selectExpr('*', 'count(*) over (partition \
                                by p_cedula_mad, p_fecha_nac , p_sexo ) as cnt').filter(F.col('cnt') >1 ).drop('cnt') 
                        re_total= re_total.selectExpr('*', 'count(*) over (partition \
                                by p_cedula_mad, p_fecha_nac , p_sexo ) as cnt').filter(F.col('cnt') == 1).drop('cnt') 
                    elif key=="id_cedulpa_fna_sexo":
                        re_auxiliar= re_total.selectExpr('*', 'count(*) over (partition \
                                by p_cedula_pad,p_fecha_nac ,p_sexo ) as cnt').filter(F.col('cnt') >1 ).drop('cnt') 
                        re_total= re_total.selectExpr('*', 'count(*) over (partition \
                                by p_cedula_pad,p_fecha_nac ,p_sexo) as cnt').filter(F.col('cnt') == 1).drop('cnt') 
                    else:
                        None
                        
                    if len(li)== 1:
                         dat_to_jog = re_total.join(re_piv, re_total[ li[0] ] == re_piv[li[0] ],"inner")\
                                         
                                        
                    elif len(li)==2 : 
                         dat_to_jog = re_total.join(re_piv, re_total[ li[0] ] == re_piv[li[0] ],"inner")\
                             .where( re_total[ li[1] ] == re_piv[li[1] ])
                         
                    elif len(li)==3:
                         dat_to_jog = re_total.join(re_piv, re_total[ li[0] ] == re_piv[li[0] ],"inner")\
                                .where( (re_total[ li[1] ] == re_piv[li[1] ] )\
                                      &  (re_total[ li[2] ] == re_piv[li[2] ]))
                                             
                    elif len(li)==4:
                          dat_to_jog = re_total.join(re_piv, re_total[ li[0] ] == re_piv[li[0] ],"inner")\
                                 .where( (re_total[ li[1] ] == re_piv[li[1] ])\
                                       &  (re_total[ li[2] ] == re_piv[li[2] ])\
                                          &  (re_total[ li[3] ] == re_piv[li[3] ]) )
                    elif len(li)==5:
                         dat_to_jog = re_total.join(re_piv, re_total[ li[0] ] == re_piv[li[0] ],"inner")\
                                .where( (re_total[ li[1] ] == re_piv[li[1] ])\
                                      &  (re_total[ li[2] ] == re_piv[li[2] ])\
                                         &  (re_total[ li[3] ] == re_piv[li[3] ])\
                                             &  (re_total[ li[4] ] == re_piv[li[4] ] ))
                                            
                                             
        
                    
                     
                     
                    re_piv = re_piv.subtract(dat_to_jog.select( [ re_piv[i]    for i in re_piv.columns]))
                    new_jo = new_jo.union(dat_to_jog\
                             .select( [re_total[Id_reg]]+ [ re_piv[i]  for i in re_piv.columns])\
                             .withColumn('id_etiqueta', F.lit(key)))
                    re_piv=re_piv.checkpoint()
                    new_jo = new_jo.checkpoint()
                    if key in ["id_cedulma_iniciale_sexo",'id_cedulpa_iniciale_sexo','id_cedulma_fna_sexo','id_cedulpa_fna_sexo']: 
                       re_total= re_total.union(re_auxiliar) # apilando codigo
                  
                    
        print("Guardando...")
                   
     
        if opcionborrar:
                             re_total = re_total.subtract( unico(re_total.join(re_piv, re_total[ li[0] ] == re_piv[li[0] ],"inner")\
                                              .select([ re_total[i]    for i in re_total.columns]))   )
                
        """construyendo un registro de resultados"""
        match_reg= new_jo.groupBy('id_etiqueta').count().toPandas()
        numdet= match_reg['count'].sum()
        
        print("Algoritmo deterministcio completado")
        
       

        ## cambio de variables y transformacion a variables de tiempo
        re_piv = tim_df( re_piv)
        re_total = tim_df( reg_pobl)
        new_jo= tim_df(  new_jo)
         
       
            
        for key in dic_var_difu.keys(): 
           print(key)
           #key='idif_sexo_fna_cedulma_N_A'
           li= dic_var_difu[key]                    
           if key== 'idif_sexo_fna_cedulma_N_A':
               dat_to_jo = re_total.join(re_piv, re_total[ li[0][0] ] == re_piv[li[0][0] ],"inner")\
                          .where(  re_total[ li[0][1] ] == re_piv[li[0][1] ]).withColumn( "coef_one",    g_funt(re_piv[li[1][0]], re_total[li[1][0]]).alias('coef_one'))\
                                              .withColumn("coef_two", g_funt(re_piv[li[1][1]], re_total[li[1][1]]))\
                                              .withColumn("coef_nombyamp", g_funt(re_piv[li[1][2]], re_total[li[1][2]]))\
                                               .withColumn("prome_nombre_ape", ((F.col("coef_one") + F.col('coef_two'))/F.lit(2)))\
                                                .withColumn("Solucion", F.least( "coef_nombyamp","prome_nombre_ape"))\
                                                .withColumn('id_etiqueta', F.lit(key))
                  
               axiliar = dat_to_jo.where(dat_to_jo.Solucion<0.20 )
               new_jo = new_jo.union(axiliar.select( [re_total[Id_reg]]+ [ re_piv[i]  \
                        for i in re_piv.columns] +[F.col('id_etiqueta')]))
               re_piv = re_piv.subtract(axiliar.select( [ re_piv[i]    for i in re_piv.columns]))                   
           
           
           elif key=='idif_sexo_fna_cedulma_N_A_30':   # para la ultima parte del algoritmo comparamos a las personas en un rango de 30 dias
               dat_to_jo = re_total.join(re_piv, re_total[ li[0][0] ] == re_piv[li[0][0] ],"inner")\
                             .withColumn( "resto_fecha",  F.abs(F.round(  re_total['p_fecha_nac'].cast("long")-re_piv['p_fecha_nac']\
                             .cast("long")) /(24*3600)) )\
                             .filter( F.col('resto_fecha') < 30 )
                             
               axiliar = dat_to_jo.withColumn( "coef_one",    g_funt(re_piv[li[1][0]], re_total[li[1][0]]).alias('coef_one'))\
                                   .withColumn("coef_two", g_funt(re_piv[li[1][1]], re_total[li[1][1]]))\
                                   .withColumn("coef_nombyamp", g_funt(re_piv[li[1][2]], re_total[li[1][2]]))\
                                    .withColumn("prome_nombre_ape", ((F.col("coef_one") + F.col('coef_two'))/F.lit(2)))\
                                     .withColumn("Solucion", F.least( "coef_nombyamp","prome_nombre_ape"))\
                                     .withColumn('id_etiqueta', F.lit(key))
               
                
               axiliar = axiliar.where(axiliar.Solucion<0.20 )
               new_jo = new_jo.union(axiliar.select( [re_total[Id_reg]]+ [ re_piv[i]  \
                        for i in re_piv.columns] +[F.col('id_etiqueta')]))
               re_piv = re_piv.subtract(axiliar.select( [ re_piv[i]    for i in re_piv.columns])) 
                
               
        
           else:
               print("NO existe sexo o fecha de nacimiento")
                
        if opcionborrar:
                    re_total = re_total.subtract( unico(axiliar.select( [ re_total[i]    for i in re_total.columns] )))
                
        #new_jo, re_piv, re_total= final_write_and_read( new_jo, re_piv, re_total )  
        re_piv=re_piv.checkpoint()
        new_jo = new_jo.checkpoint()
        
        """ recolectando datos"""
        
        new_jo.coalesce(1).write.mode("overwrite").option("header","true").csv(path_location+'solucion/'+'mach_join.csv')      
        re_piv.coalesce(1).write.mode("overwrite").option("header","true").csv(path_location+'solucion/'+'mach_pivot.csv')      
        if opcionborrar:
             re_total.coalesce(1).write.mode("overwrite").option("header","true").csv(path_location+'solucion/'+'mach_reg.csv')      

        
        
        """construyendo un registro de resultados"""
        
        
        
        refif=new_jo.groupBy('id_etiqueta').count().toPandas()
        match_reg=pd.concat( [match_reg, refif])
        
        
        df1= pd.DataFrame([['total deterministico',  numdet]], columns=['id_etiqueta', 'count'])
        match_reg=pd.concat( [match_reg, df1]) 
        
        numdif= new_jo.count()-numdet
        df2= pd.DataFrame([['total difuso',  numdif]], columns=['id_etiqueta', 'count'])
        match_reg=pd.concat( [match_reg, df2])
        
        df4= pd.DataFrame([['total encontrados',  numdet+numdif]], columns=['id_etiqueta', 'count'])
        match_reg=pd.concat( [match_reg, df4]) 
        
        
        df3= pd.DataFrame([['No encontrados',  re_piv.count()]], columns=['id_etiqueta', 'count'])
        match_reg=pd.concat( [match_reg, df3])
        
        match_reg.to_csv( path_location+'Informacion.csv' ,index= 'false')
        spark.cleaner.referenceTracking.cleanCheckpoints(True)
        
        
        print(match_reg)
        
        








