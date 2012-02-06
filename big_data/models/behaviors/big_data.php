<?php
  /******************************************
  // Author: Jarriett K Robinson
  // Email: jarriett@gmail.com
  ******************************************/
  
  class BigDataBehavior extends ModelBehavior
  {
      var $Model;
      
    //  var $bundle = array();
      
      var $COMPARISON_SYMBOLS = array( 
                                     '<=' => '<=', 
                                     '>=' => '>=', 
                                     '!='  => '!=',
                                     '<' => '<',
                                     '>' => '>' , 
                                     'NOT LIKE' => 'NOT LIKE',
                                     'LIKE' => 'LIKE',
                                     '=' => '='
                                     );
      var $KEYWORDS = array("OR", "NOT");
      
      var $primary_key_field;
      
      var $debugging;
                                     
      public function setup(&$Model, $config = array())
      {
        $this->Model = $Model;
        $bundle  = array($Model->name => array()); 
        
        //determine the primary key field
        foreach($this->Model->_schema as $col => $info)
        {
          if(!empty($info['key']) && $info['key'] == 'primary')
        	{
        		$this->primary_key_field = $col;
        		break;
        	}
        }
      }
      
      public function addToBundle(&$m, array &$model_data = array())
      {  
          $this->Model = $m;
         if(empty($model_data[$m->name]) && !is_array($model_data[$m->name]))
          {
             $this->Model->bundle[] = $this->fillCompleteSchema($model_data);  
          }
          else
          {
              $this->Model->bundle[] = $this->fillCompleteSchema($model_data[$m->name]);
          }      
      }
      
      public function saveBundle(&$model, $max_payload = 100000, $replace = true)
      {
          $this->Model = $model; 
          if(count($this->Model->bundle) > $max_payload)
          {
              $chunked = array_chunk($this->Model->bundle, $max_payload);
              $this->Model->bundle = null;
              $this->Model->bundle = array();
              foreach($chunked as $chunk)
              {
                  $this->bulkSave($model, $chunk, $replace);
              }          
          }
          else
          {
            $this->bulkSave($model, $this->Model->bundle, $replace);
          }
          $this->Model->bundle = null;
          $this->Model->bundle = array();    
      }
      
      function bulkSave(&$model, array &$datas = array(), $replace = true)
      {
          $this->Model = $model;
          $name = $this->Model->name;
          $table = Inflector::tableize($name);
          $field_names = array_keys($this->Model->_schema);
          $field_data = array();
          foreach($field_names as $field_name)
          {
              $field_data[$field_name] = array();
          }           
          $save_cols = array();
          !empty($datas[0]) && is_array($datas[0]) ? $save_keys = array_keys($datas[0]) : $save_keys = array();

          foreach($field_names as $_col)
          {
              if(is_array($save_keys) && in_array($_col, $save_keys))
              {
                  $save_cols[] = $_col;
              }
              
          }
          if(!empty($datas) && count($datas) > 0)
          {
              $sql = "INSERT INTO `{$table}` (";
              for($i = 0; $i < count($field_names); $i++)
              {
                   if(in_array($field_names[$i], $save_cols))
                   {
                       $sql .= $field_names[$i];
                       if(count($field_names) > $i + 1)
                       {
                           $sql .= ", ";
                       }
                       else
                       {
                           $sql .= ") ";
                       }
                   }
                   else
                   {
                       
                   }                   
              }           
              $values = "VALUES(";   
              
         
              $basecount = count($datas[0]);  //basecount is the amount of elements in the first object.  All model objects must have the same number of properties
              
              //the model object cannot have more properties than columns specified on the database schema
              if($basecount > count($this->Model->_schema))
              {
                  throw new Exception("ERROR - The dataset cannot have more properties than there are columns on the database table.  Available columns: " . array_keys($this->Model->_schema) . " - Submitted properties: " . array_keys($datas[0]));
              }
                         
              foreach($datas as $data)
              {
                  if(count($data) != $basecount)
                  {   
                      echo "\n-------------------------------------------------------------------------------------------------\n";
                      echo ("\nWARNING - ".$this->Model->name."->BigData->bulkSave() - Each dataset to be saved must contain the same number of values: BigData->bulkSave() - Base Count: {$basecount} : Columns - ".implode(",",array_keys($datas[0]))." - Submitted Data: ".implode(",",array_keys($data))) ."\n";
                      echo "\n-------------------------------------------------------------------------------------------------\n";
                  }
                  foreach($data as $field_name => $value)
                  {
                    $field_data[$field_name][] = $value;  
                  }  
              }     
              $y = 0; 
              
              $num_records = count($datas);
              for($cnt = 0; $cnt < $num_records; $cnt++)
              {   
                 $x = 0;
                 foreach($save_cols as $col)
                 {   
                 
                     if((empty($field_data[$col][$cnt]) ||$field_data[$col][$cnt] == 'NULL')  && $field_data[$col][$cnt] != 0)
                     {
                         $values .= 'NULL'; 
                     }
                     else
                     { 
                         if($this->Model->_schema[$col]['type'] == "string" || $this->Model->_schema[$col]['type'] == "date")
                         {
                             $values .= '"'.$field_data[$col][$cnt].'"';
                         }
                         else
                         {
                             $values .= $field_data[$col][$cnt];
                         }
                     }
                     if(count($save_cols) -1 >= $x + 1)
                     {
                         $values .= ", ";
                     }
                     else
                     {
                         $values .= ") ";
                     }
                     $x++;
                 }
                 if(count($field_data[$save_cols[0]]) - 1 >= $cnt + 1)
                 {
                     $values .= ", (";
                 }
                 $y++; 
              } 
              $sql .= $values;
              
              if($replace === true)
              {  
                 $r = 0;
                 $update = "";
                 $sql .= " ON DUPLICATE KEY UPDATE ";
                 foreach($save_cols as $rCol)
                 {
                   if(!empty($field_data[$rCol]))
                   {
                        $update .= "{$rCol} = VALUES({$rCol})";
                        if(count($save_cols) -1 >= $r +1)
                        {
                            $update .= ", ";
                        }
                        
                        $field_data[$rCol] = null;  
                   }
                   $r++;
                 }
                 $sql .= $update;   
               
              }
              
              $sql .= ";"; 
              
              //if debugging on, print query
              if($this->debugging == true)
              {
                  echo "\nDEBUGGING - BigData->bulkSave() query for ".$this->Model->name."\n------------------------------------------------------------------------------------------------------\n"
                            . $sql. "\n------------------------------------------------------------------------------------------------------\n";
              }
              $this->Model->query($sql);
          }
      }
      
      public function getBundle()
      {
          return $this->Model->bundle;
      }
      
      /**
      * fetchHashedResult
      * 
      * @param mixed $model
      * @param mixed $query
      *     array(
      *             'key' = array() - an array of field names to be comrpised as the array key,
      *             'useHash' = boolean - a boolean flag specifying if the key string should be returned as an MD5 hash, false by default 
      *             'fields' = array() - an array of field names to be returned in the result,
      *             'conditions' = array() - an array of field => value conditions, same as cakePHP's normal find(),  
      *             'group' = array() - an array of fields to group the results by,         
      *             'limit' = integer - an integer value to impose as the maximum number of results
      *           )
      */
      public function fetchHashedResult(&$model, $query = array())
      {
          $this->Model = $model;
          $name = $this->Model->name;
          $table = Inflector::tableize($name);
          $field_names = array_keys($this->Model->_schema);
          $keyStr = $fieldStr = $groupStr = $limitStr = $conditionStr = $sql = '';
          if(!empty($query['key']) && is_array($query['key']))
          {
            for($i=0; $i< count($query['key']); $i++)
            {
                $keyStr .= $query['key'][$i];
                if($i+1 < count($query['key']))
                {
                    $keyStr .= ",";
                }
            }
            $keyStr = "CONCAT(" . $keyStr . ")";
            if(!empty($query['useHash']) && $query['useHash'] == true)
            {
                $keyStr = "MD5(". $keyStr .")";
            }
            
            $keyStr.= " as `hash_id` ";  
          }
          elseif(!empty($keyStr))
          {
              $keyStr .= " as `hash_id` ";
          }
          else
          {
              throw new Exception("[ERROR] A key must be specified when calling BigData->fetchHashedResult()");
          }
          if(!empty($query['fields']) && is_array($query['fields']))
          {
              for($i=0; $i < count($query['fields']); $i++)
              {
                $fieldStr .= $query['fields'][$i];
                if($i + 1 < count($query['fields']))
                {
                    $fieldStr .= ",";
                }  
              }
          }
          elseif(!empty($query['fields']))
          {
              $fieldStr = $query['fields'];
          }
          else
          {
              for($i=0; $i < count($field_names); $i++)
              {
                  $fieldStr .= $field_names[$i];
                  if($i + 1 < count($field_names))
                  {
                      $fieldStr .= ",";
                  }
              }
          }
          
          if(array_key_exists('conditions', $query) && is_array($query['conditions']))
          {
              if( !empty($query['conditions'])&& !is_array($query['conditions'][0]))
              {
                  $i = 0;
                  foreach($query['conditions'] as $col => $value)
                  {
                      if(!in_array($col, $this->KEYWORDS))
                      {
                          $conditionStr .= $this->buildEval($col, $value);
                          
                          if($i + 1 < count($query['conditions']))
                          {
                              $conditionStr .= " AND ";
                          }
                          ++$i;
                      }
                  }
              }
             
              if(!empty($query['conditions']['OR']) && is_array($query['conditions']['OR']))
              {
                  $i = 0;
                  foreach($query['conditions']['OR'] as $col => $value)
                  {
                     $conditionStr .= $this->buildEval($col, $val);
                     if($i + 1 < count($query['conditions']['OR']))
                     {
                         $conditionStr .= " OR ";
                     }
                     ++$i;
                  }
              }  
              
              if(!empty($query['conditions']['NOT']) && is_array($query['conditions']['NOT']))
              {
                  
                  $notStr = "";
                  foreach($query['conditions']['NOT'] as $col => $values)
                  { 
                      $notStr =  " ".$col. " NOT IN (";
                      
                     if(is_array($values))
                     {
                         foreach($values as $value)
                         {
                             $notStr .=  $value. ", ";
                         }
                         $tmpNotStr = rtrim(rtrim($notStr), ","); 
                         $notStr = $tmpNotStr . ")"; 
                     } 
                    
                     else
                     {
                         $notStr .= $values . ")";
                     }
                     
                  }
                  
                  if($notStr != "")
                  {
                      if(empty($conditionStr) || (substr(rtrim($conditionStr), (strlen(rtrim($conditionStr) - 3)), strlen(rtrim($conditionStr)) != "AND")))
                      {
                            $conditionStr .= $notStr;
                      }
                      else
                      {
                            $conditionStr .= " AND "  . $notStr;   
                      }
                  }
              }
              
              $conditionStr = " WHERE " . $conditionStr;
          }
          if(!empty($query['group']) && is_array($query['group']))
          {
              for($i=0; $i < count($query['group']); $i++)
              {
                  $groupStr .= $query['group'][$i];
                  if($i + 1 < count($query['group']))
                  {
                      $groupStr .= ",";
                  }
              }
              $groupStr = "GROUP BY ". $groupStr;
          }
          elseif(!empty($query['group']))
          {
              $groupStr = "GROUP BY ".$query['group'];
          }
          else
          {
              $groupStr = "";
          }
          if(!empty($query['limit']))
          {
              $limitStr = "LIMIT ".$query['limit'];
          }
          
          $sql = 'SELECT '. $fieldStr. ', '. $keyStr.' FROM '.$table  . ' AS  '. $name. ' ' . $conditionStr . ' ' . $groupStr . ' ' . $limitStr .";";
          
          if($this->debugging == true)
          {
              echo "\nDEBUGGING - BigData->fetchHashedResult() query for ". $this->Model->name. "\n------------------------------------------------------------------------------------------------------\n"
                                                                                                                                                        .$sql."\n------------------------------------------------------------------------------------------------------\n";
          }

          $results = $this->Model->query($sql);
          
          $hash_result = array();
          foreach($results as $result)
          {
            $tmp = array();
            foreach($result[$name] as $key => $value)
            {
                $tmp[$key] = $value;
            }
            if(count($result[0]) > 1)
            {
                foreach($result[0] as $key => $value)
                {
                    if($key != 'hash_id')
                    {
                        $tmp[$key] = $value;
                    }
                }
            }
            $hash_result[$result[0]['hash_id']] = $tmp;  
          }
          $result = null;
          return $hash_result;
      }
      
      /**
      * convertResultToHash
      * 
      * @param mixed $model
      * @param mixed $result = array. The result array returned from a query or find() operation
      * @param mixed $key = array.  The values of each result that will act as the hash key.
      *                             for nested arrays (example: result[r1][r2][r3]) express the key as a string of "r1.r2.r3"
      */
      function convertResultToHash(&$model, array &$result = array(), $key = array())
      {
          $this->Model = $model;
          if(empty($key))
          {
              throw new Exception("ERROR - A key value must be specified when calling BigData->convertResultToHash()");
          }
          
          $hash = array();
          foreach($result as $r)
          {
              //build the key using fields in the result array
              $kStr = "";
              foreach($key as $k)
              {
                  if(strpos($k, ".") !== false)
                  {
                      $fields = explode(".",$k);
                      //only fetch nested keys up to 3 levels
                      if(count($fields) == 2)
                      {
                          $kStr .= $r[$fields[0]][$fields[1]];
                      }
                      if(count($fields) == 3)
                      {
                          $kStr .= $r[$fields[0]][$fields[1]][$fields[2]];
                      }                      
                  }
                  else
                  {
                      $kStr .= $r[$k];
                  }
              }
              
              $hash[$kStr] = $r;
          }
          return $hash;
      }
      
      private function generateEmptyValue($column)
      {
          $colinfo = $this->Model->_schema[$column];
          $blank = "NULL";
          if($colinfo['null'] == 1)
          {
              return $blank;
          }
          switch($colinfo['type'])
          {
              case "string":
                empty($colinfo['default']) ? $blank = "" : $blank = $colinfo['default'];
                break;
              case "date":
               empty($colinfo['default']) ? $blank = date("Y-m-d") : $blank = $colinfo['default'];
               break;
              case "datetime":
                empty($colinfo['default']) ? $blank = date("Y-m-d H:i:s") : $blank = $colinfo['default'];
                break;
              default:
                 empty($colinfo['default']) ? $blank = 0 : $blank = $colinfo['default'];
                 break;              
          }
          return $blank;
      }
      
      private function fillCompleteSchema(array $model_data)
      {
          $missingFields = array();
          $field_names = array_keys($this->Model->_schema);  
          $data_keys = array_keys($model_data);
          foreach($field_names as $field_name)
          {    
            if(!in_array($field_name, $data_keys))
            {
                //==========================================================================================
                // Note:  If the primary key is not included, it will not be added.  However, if the primary key field
                //            is included, it will not be removed either.  This key would simply be used to update on duplicate key
                //==========================================================================================
                if(array_key_exists('key', $this->Model->_schema[$field_name]))
                {
                    if($this->Model->_schema[$field_name]['key'] == 'primary') 
                    {
                        //skip primary keys
                    }
                    else
                    {
                      $missingFields[$field_name] = $this->generateEmptyValue($field_name);   
                    }
                }
                else
                {
                   $missingFields[$field_name] = $this->generateEmptyValue($field_name); 
                }  
           }   
          }            //clean the object by removing illegitimate fields                 
          return $this->removeRogueFields(array_merge($model_data, $missingFields));
      }
      
      private function removeRogueFields(array $model_data)
      {
          $clean_data = array();
          $schema_keys = array_keys($this->Model->_schema);
          foreach($model_data as $key => $value)
          {
              // only add fields that actually exist on the schema - avoid MySQL column does not exist exceptions
              if(in_array($key, $schema_keys))
              {
                  $clean_data[$key] = $value;
              }
          }
          return $clean_data;
      }
      
    private function makeSafe($text)
    {
        $t2 = $text;
        str_replace('"', "", $t2 );
        
        return $t2;
    }
    
    private function buildEval($field, $expr)
    {
        $f = trim($field);
        foreach($this->COMPARISON_SYMBOLS as $key => $value)
        {   
            if(strpos($f, $value) !== false)
            {
                return $f.' ' . $this->makeComparison($f,$expr); 
            }
        }
        if(is_array($expr))
        {
             $eval = $field . " IN(";     
             foreach($expr as $e)
             {   
                $eval .= $this->makeComparison($field, $e) .','; 
             }
             $clean_eval = rtrim($eval, ",");
             $clean_eval .= ")"; 
             return $clean_eval;
        }
            
        return $field." = ". $this->makeComparison($field, $expr);     
     }
     
     private function makeComparison($raw_field, $expr)
     {  
        //safe guard in case fields are specified as table.field
        $field = $raw_field;
        if(strpos($raw_field, "."))
        {
          $field = substr($raw_field, strpos($raw_field, ".") +1, strlen($raw_field) -1);  
        }
        if(!empty($this->Model->_schema[$field]) && !empty($this->Model->_schema[$field]['type']))
        { 
            switch($this->Model->_schema[$field]['type'])  
            {
                case 'integer':
                case 'float': 
                   return $this->makeSafe($expr);
                   break;
                default:
                   return '"' . $this->makeSafe($expr) .'"';
                   break;
            }
        }
        else
        {    
            foreach($this->COMPARISON_SYMBOLS as $sym)
            {
                if(stripos($field, $sym) != FALSE)
                {     
                    $tempField = str_replace($sym, "", $field); //remove the special character and spaces
                    $field = str_replace(" ", "", $tempField);
                    return $this->makeComparison($field, $expr);
                }
            }
             return '"' . $this->makeSafe($expr) .'"';
        }
     }
     
     public function setDebugging($debugging = false)
     {
         $this->debugging = $debugging;
     }
     
     public function getDebugging()
     {
         return $this->debugging;
     }
     
    private function is_associative_array($a)
    {
        foreach(array_keys($a) as $key)
        {
            if (!is_int($key)) return TRUE;
        }
        return FALSE;
    }

  }
?>