var http = require('http');
var express = require('express');
var app = express();
var axios = require('axios');
var objGov = new Array();
const { Connection, Request } = require("tedious");
var TYPES = require('tedious').TYPES;
var contGeneral = 0;


// Create connection to database
const config = {
  authentication: {
    options: {
      userName: "desarrollonk@bx8awnh5ce", // update me
      password: "(n3tm3d1k)" // update me
    },
    type: "default"
  },
  server: "bx8awnh5ce.database.windows.net", // update me
  options: {
    database: "netmedik2", //update me
    encrypt: true
  }
};



app.get('/medicamentos', function(req, res) {
        
});

function consultarAPI(offset){
    var config = {
      method: 'get',
      url: 'https://www.datos.gov.co/resource/i7cb-raxc.json?$offset='+offset,
      headers: { 
        '$$app_token': '4pmpq9h5chahw3ayt4s48qbi4y3ddei6d65ceeip88jde1b2wu'
      }
    };
    axios(config)
        .then(function (response) {
          //console.log(JSON.stringify(response.data));

          //console.log(json.length);
            apilarObjeto(response.data,offset);
          //res.send(JSON.stringify(response.data));
        })
        .catch(function (error) {
          console.log(error);
        });
}


function apilarObjeto(data,offset){

   if(data.length != 0){
   //if(offset != 1000){
        objGov.push(data);
        offset+=1000;
        console.log('offset ->'+offset);
        consultarAPI(offset);
        //setTimeout(function(){ consultarAPI(offset); }, 2000);
        
    }else{
        //recorrerObejto(objGov);
        recorrerObjFase1(objGov,0);
    }
   
}

function recorrerObejto(object){

    var cont = 0;

    while(cont < object.length){
          console.log(object[cont].length);  
          cont++;
    }
    console.log(cont);  
}


function recorrerObjFase1(objGov,x){

   
    if(x < objGov.length){
        recorrerObjFase2(objGov,x,0);
    
    }else{
        console.log('Termino');
    }
       
}

function recorrerObjFase2(objGov,x,y){

   
    if(y < objGov[x].length)
    {
        cosultarMedicamentosNetmedikUnoaUno(objGov,x,y);
    }else{
        x++;
        recorrerObjFase1(objGov,x);
    }
       
}


function cosultarMedicamentosNetmedikUnoaUno(objGov,x,y){
    //console.log("SELECT * FROM dbo.medicamentos WHERE [EXPEDIENTE] = '"+objGov[x][y]['expediente']+"' AND [PRODUCTO] = '"+objGov[x][y]['producto']+"' AND [TITULAR] = '"+objGov[x][y]['titular']+"' AND [DESCRIPCION PRESENTACION COMERCIAL] = '"+objGov[x][y]['descripcioncomercial']+"'  AND [DESCRIPCION ATC] = '"+ objGov[x][y]['descripcionatc'] +"' AND  [PRINCIPIO ACTIVO] = '"+objGov[x][y]['principioactivo']+"' AND  [NOMBRE DE ROL] = '"+ objGov[x][y]['nombrerol'] +"'");
    const connection = new Connection(config);
    connection.on("connect", err => {
        if (err) {
            console.error(err.message);
        } else {
            //console.log('entro al request');
            const request = new Request(
            "SELECT * FROM dbo.medicamentos WHERE [EXPEDIENTE] = '"+objGov[x][y]['expediente']+"' AND [PRODUCTO] = '"+objGov[x][y]['producto']+"' AND [TITULAR] = '"+objGov[x][y]['titular']+"' AND [DESCRIPCION PRESENTACION COMERCIAL] = '"+objGov[x][y]['descripcioncomercial']+"'  AND [DESCRIPCION ATC] = '"+ objGov[x][y]['descripcionatc'] +"' AND  [PRINCIPIO ACTIVO] = '"+objGov[x][y]['principioactivo']+"' AND  [NOMBRE DE ROL] = '"+ objGov[x][y]['nombrerol'] +"'",
            (err, rowCount) => {
              if (err) {
                console.error(err.message);
              } else {
                //console.log(`${rowCount} row(s) returned`);

                if(rowCount == 0){
                    //console.log('Se Inserta');
                    connection.close();
                    InsertarActualizarMedicamento(objGov,x,y,1);
                }else{
                    //console.log('Se Actualiza');
                    connection.close();
                    InsertarActualizarMedicamento(objGov,x,y,2);
                }
              }
            }
          );
            connection.execSql(request);
          }
    });       
}


function InsertarActualizarMedicamento(objGov,x,y,action) {

    const connection2 = new Connection(config);
    connection2.on("connect", err => {
      if (err) {
            console.error(err.message);
          } else {
            Insertquery();
          }
        });

        function Insertquery() {
          //console.log("Insertando datos...");

              const requestInsert = new Request(
                "exec sp_ingresar_actualizar_medicamentos_gobierno @EXPEDIENTE, @PRODUCTO, @TITULAR, @REGISTRO_SANITARIO, @FECHA_EXPEDICIÓN, @FECHA_VENCIMIENTO, @ESTADO_REGISTRO, @Campo8, @CONSECUTIVO, @CANTIDAD, @DESCRIPCION_PRESENTACION_COMERCIAL, @ESTADO_CUM, @FECHA_ACTIVO, @FECHA_INACTIVO, @MUESTRA_MEDICA, @UNIDAD, @ATC, @DESCRIPCION_ATC, @VIA_ADMINISTRACION, @CONCENTRACION, @PRINCIPIO_ACTIVO, @UNIDAD_MEDIDA, @Campo23, @UNIDAD_REFERENCIA, @FORMA_FARMACEUTICA, @TIPO_DE_ROL, @NOMBRE_DE_ROL, @MODALIDAD, @Campo29, @action",
                (err, rowCount) => {
                  if (err) {
                    console.error(err.message);
                  } else {
                    //console.log('Paso Base de Datos');
                    connection2.close();
                    y++;
                    contGeneral++;
                    console.log(contGeneral);
                    recorrerObjFase2(objGov,x,y);
                  }

                }
              );
               
                requestInsert.addParameter('EXPEDIENTE', TYPES.Float, objGov[x][y]['expediente'] );
                requestInsert.addParameter('PRODUCTO', TYPES.VarChar, objGov[x][y]['producto'] );
                requestInsert.addParameter('TITULAR', TYPES.VarChar, objGov[x][y]['titular'] );
                requestInsert.addParameter('REGISTRO_SANITARIO', TYPES.VarChar, objGov[x][y]['registrosanitario'] );
                requestInsert.addParameter('FECHA_EXPEDICIÓN ', TYPES.DateTime, objGov[x][y]['fechaexpedicion'] );
                requestInsert.addParameter('FECHA_VENCIMIENTO ', TYPES.DateTime, objGov[x][y]['fechavencimiento'] );
                requestInsert.addParameter('ESTADO_REGISTRO', TYPES.VarChar, objGov[x][y]['estadoregistro'] );
                requestInsert.addParameter('Campo8', TYPES.Float, objGov[x][y]['expedientecum'] );
                requestInsert.addParameter('CONSECUTIVO', TYPES.Float, objGov[x][y]['consecutivocum'] );
                requestInsert.addParameter('CANTIDAD', TYPES.Float, objGov[x][y]['cantidadcum'] );
                requestInsert.addParameter('DESCRIPCION_PRESENTACION_COMERCIAL', TYPES.VarChar , objGov[x][y]['descripcioncomercial'] );
                requestInsert.addParameter('ESTADO_CUM', TYPES.VarChar, objGov[x][y]['estadocum'] );
                requestInsert.addParameter('FECHA_ACTIVO', TYPES.DateTime, objGov[x][y]['fechaactivo'] );
                requestInsert.addParameter('FECHA_INACTIVO', TYPES.DateTime, objGov[x][y]['fechainactivo'] );
                requestInsert.addParameter('MUESTRA_MEDICA', TYPES.VarChar, objGov[x][y]['muestramedica'] );
                requestInsert.addParameter('UNIDAD', TYPES.VarChar, objGov[x][y]['unidad'] );
                requestInsert.addParameter('ATC', TYPES.VarChar, objGov[x][y]['atc'] );
                requestInsert.addParameter('DESCRIPCION_ATC', TYPES.VarChar, objGov[x][y]['descripcionatc'] );
                requestInsert.addParameter('VIA_ADMINISTRACION', TYPES.VarChar, objGov[x][y]['viaadministracion'] );
                requestInsert.addParameter('CONCENTRACION', TYPES.VarChar, objGov[x][y]['concentracion'] );
                requestInsert.addParameter('PRINCIPIO_ACTIVO', TYPES.VarChar, objGov[x][y]['principioactivo'] );
                requestInsert.addParameter('UNIDAD_MEDIDA', TYPES.VarChar,  objGov[x][y]['unidadmedida']);
                requestInsert.addParameter('Campo23', TYPES.Float, objGov[x][y]['cantidad'] );
                requestInsert.addParameter('UNIDAD_REFERENCIA', TYPES.VarChar, objGov[x][y]['unidadreferencia'] );
                requestInsert.addParameter('FORMA_FARMACEUTICA', TYPES.VarChar, objGov[x][y]['formafarmaceutica'] );
                requestInsert.addParameter('TIPO_DE_ROL', TYPES.VarChar, objGov[x][y]['tiporol'] );
                requestInsert.addParameter('NOMBRE_DE_ROL', TYPES.VarChar, objGov[x][y]['nombrerol'] );
                requestInsert.addParameter('MODALIDAD', TYPES.VarChar, objGov[x][y]['modalidad'] );
                requestInsert.addParameter('Campo29', TYPES.VarChar, null );
                requestInsert.addParameter('action', TYPES.Int, action);

                //console.log('Expediente -> '+objDataInsert[contInsert]['expediente']);
                connection2.execSql(requestInsert);
        }

}






















function cosultarMedicamentosNetmedik(objGov) {
    const connection = new Connection(config);
    var allRows = "[";
    var resp = "[{";
    connection.on("connect", err => {
      if (err) {
            console.error(err.message);
          } else {
            queryDatabase(objGov);
          }
        });

        function queryDatabase(objGov) {
          console.log("Reading rows from the Table...");

          // Read all rows from table
          const request = new Request(
            `SELECT * FROM dbo.medicamentos`,
            (err, rowCount) => {
              if (err) {
                console.error(err.message);
              } else {
                console.log(`${rowCount} row(s) returned`);
              }
            }
          );


          request.on('row', function(columns) {
            var rep = "{";
            columns.forEach(function(column) {
                if(column.metadata.colName != undefined || column.metadata.colName != ''){
                    var vJson = column.value;
                    if(column.metadata.colName == 'NOMBRE DE ROL'){
                       var vJ = vJson.toString().replace(/['"]+/g, "");
                    }                    
                    rep += '"'+column.metadata.colName +'":"'+ vJ +'"';
                    if(column.metadata.colName != 'id_laboratory'){
                        rep += ",";
                    }
                }
                 
            });
            rep += '}';
            allRows += rep+','; 
        });

          request.on('doneProc', function (rowCount, more, returnStatus, rows) {
            allRows = allRows.substring(0, allRows.length - 1);
            allRows += ']';
            allRows = allRows.replace(/^\ufeff/g,"");
            objJson = JSON.parse(allRows.trim());
            recorrerDataNetmedik(objJson, objGov, 0);
     
            //recorrerDataNetmedik(JSON.parse(allRows));
        });


         connection.execSql(request);
        }

  
}

function recorrerDataNetmedik(objNet, objGov, x){
    console.log('entro a recorrerDataNetmedik');
    
    if(x < objGov.length){
        var y = 0;
        while(y < objGov[x].length)
        {
            var i = 0;
            var valUp = 0;
            while(i < objNet.length){
                if(objGov[x][y]['expediente'] == objNet[i]['EXPEDIENTE']  &&
                    objGov[x][y]['producto'] == objNet[i]['PRODUCTO']  &&
                    objGov[x][y]['titular'] == objNet[i]['TITULAR'] &&
                    objGov[x][y]['descripcioncomercial']  == objNet[i]['DESCRIPCION PRESENTACION COMERCIAL']  &&
                    objGov[x][y]['descripcionatc'] == objNet[i]['DESCRIPCION ATC']  &&
                    objGov[x][y]['principioactivo'] == objNet[i]['PRINCIPIO ACTIVO']  &&
                    objGov[x][y]['nombrerol'] == objNet[i]['NOMBRE DE ROL'])
                {
                    //console.log(cUpdate+' Actualiza');
                    cUpdate++;
                    valUp = 1;
                 
              
                    objDataUpdate.push(objGov[x][y]);
                    //InsertarActualizarMedicamento(objGov,x,y);
                }
                    i++;
                
            }
            if(valUp == 0)
            {
                //console.log(cInsert+' Inserta');
                cInsert++;
                objDataInsert.push(objGov[x][y]);
            }
            y++;
            
        }
        
        x++;
        recorrerDataNetmedik(objNet, objGov, x); 
    }else{
        console.log('Termino x -> '+ x);
        console.log('Termino contGeneral -> '+ contGeneral);
        console.log('Actualiza cUpdate -> '+ cUpdate);
        console.log('Inserta cInsert -> '+ cInsert);
        InsertarMedicamento();

    }
       
}




function ActualizarMedicamento() {
    console.log("ingresa a actualizar medicamento");
    console.log("Canitdad de Actualizacion -> "+ objDataUpdate.length);
    console.log("finnnnnnn");
    /*const connection2 = new Connection(config);
    connection2.on("connect", err => {
      if (err) {
            console.error(err.message);
          } else {
            console.log("entra al else");
            Insertquery(registro_medico,user_id,obj);
          }
        });

        function Insertquery(registro_medico,user_id,obj) {
          console.log("Insertando datos...");

          // Read all rows from table
          const request = new Request(
            "exec sp_ingresar_actualizar_registro_medico @user_id,@registro,@valor",
            (err, rowCount) => {
              if (err) {
                console.error(err.message);
              } else {
                console.log(`${rowCount} row(s) returned`);
                cont++;
                connection2.close();
                setTimeout(function(){ consultarAPI(obj,cont); }, 2000);
              }

            }
          );
          request.addParameter('user_id', TYPES.Int,user_id);
          request.addParameter('registro', TYPES.VarChar,'Registro Medico');
          request.addParameter('valor', TYPES.VarChar,registro_medico);


         connection2.execSql(request);
        }*/

  
}


app.listen(3100, function() {
  console.log('puerto 3100!');
  consultarAPI(0);
  //cosultarMedicamentosNetmedik();
});