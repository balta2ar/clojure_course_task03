(ns clojure-course-task03.core
  (:require [clojure.set])
  (:require [clojure.string :as string])
  (:use [clojure.pprint]))

(defn join* [table-name conds]
  (let [op (first conds)
        f1 (name (nth conds 1))
        f2 (name (nth conds 2))]
    (str table-name " ON " f1 " " op " " f2)))

(defn where* [data]
  (let [ks (keys data)
        res (reduce str (doall (map #(let [src (get data %)
                                           v (if (string? src)
                                               (str "'" src "'")
                                               src)]
                                       (str (name %) " = " v ",")) ks)))]
    (reduce str (butlast res))))

(defn order* [column ord]
  (str (name column)
       (if-not (nil? ord) (str " " (name ord)))))

(defn limit* [v] v)

(defn offset* [v] v)

(defn -fields** [data]
  (reduce str (butlast (reduce str (doall (map #(str (name %) ",") data))))))

(defn fields* [flds allowed]
  (let [v1 (apply hash-set flds)
        v2 (apply hash-set allowed)
        v (clojure.set/intersection v1 v2)]
    (cond
     (and (= (first flds) :all) (= (first allowed) :all)) "*"
     (and (= (first flds) :all) (not= (first allowed) :all)) (-fields** allowed)
     (= :all (first allowed)) (-fields** flds)
     :else (-fields** (filter v flds)))))

(defn select* [table-name {:keys [fields where join order limit offset]}]
  (-> (str "SELECT " fields " FROM " table-name " ")
      (str (if-not (nil? where) (str " WHERE " where)))
      (str (if-not (nil? join) (str " JOIN " join)))
      (str (if-not (nil? order) (str " ORDER BY " order)))
      (str (if-not (nil? limit) (str " LIMIT " limit)))
      (str (if-not (nil? offset) (str " OFFSET " offset)))))


(defmacro select [table-name & data]
  (let [;; Var containing allowed fields
        fields-var# (symbol (str table-name "-fields-var"))

        ;; The function takes one of the definitions like (where ...) or (join ...)
        ;; and returns a map item [:where (where* ...)] or [:join (join* ...)].
        transf (fn [elem]
                 (let [v (first elem)
                       v2 (second elem)
                       v3 (if (> (count elem) 2) (nth elem 2) nil)
                       val (case v
                               fields (list 'fields* (vec (next elem)) fields-var#)
                               offset (list 'offset* v2)
                               limit (list 'limit* v2)
                               order (list 'order* v2 v3)
                               join (list 'join* (list 'quote v2) (list 'quote v3))
                               where (list 'where* v2))]
                   [(keyword v) val]))

        ;; Takes a list of definitions like '((where ...) (join ...) ...) and returns
        ;; a vector [[:where (where* ...)] [:join (join* ...)] ...].
        env* (loop [d data
                    v (first d)
                    res []]
               (if (empty? d)
                 res
                 (recur (next d) (second d) (conj res (transf v)))))

        ;; Accepts vector [[:where (where* ...)] [:join (join* ...)] ...],
        ;; returns map {:where (where* ...), :join (join* ...), ...}
        env# (apply hash-map (apply concat env*))]
    
    `(select* ~(str table-name)  ~env#)))


;; Examples:
;; -------------------------------------

(let [proposal-fields-var [:person, :phone, :address, :price]]
  (select proposal
          (fields :person, :phone, :id)
          (where {:price 11})
          (join agents (= agents.proposal_id proposal.id))
          (order :f3)
          (limit 5)
          (offset 5)))

(let [proposal-fields-var [:person, :phone, :address, :price]]
  (select proposal
          (fields :all)
          (where {:price 11})
          (join agents (= agents.proposal_id proposal.id))
          (order :f3)
          (limit 5)
          (offset 5)))

(let [proposal-fields-var [:all]]
  (select proposal
          (fields :all)
          (where {:price 11})
          (join agents (= agents.proposal_id proposal.id))
          (order :f3)
          (limit 5)
          (offset 5)))

(comment
  ;; Описание и примеры использования DSL
  ;; ------------------------------------
  ;; Предметная область -- разграничение прав доступа на таблицы в реелтерском агенстве
  ;;
  ;; Работают три типа сотрудников: директор (имеет доступ ко всему), операторы ПК (принимают заказы, отвечают на тел. звонки,
  ;; передают агенту инфу о клиентах), агенты (люди, которые лично встречаются с клиентами).
  ;;
  ;; Таблицы:
  ;; proposal -> [id, person, phone, address, region, comments, price]
  ;; clients -> [id, person, phone, region, comments, price_from, price_to]
  ;; agents -> [proposal_id, agent, done]

  ;; Определяем группы пользователей и
  ;; их права на таблицы и колонки
  (group Agent
         proposal -> [person, phone, address, price]
         agents -> [clients_id, proposal_id, agent])

  ;; Предыдущий макрос создает эти функции
  (select-agent-proposal) ;; select person, phone, address, price from proposal;
  (select-agent-agents)  ;; select clients_id, proposal_id, agent from agents;




  (group Operator
         proposal -> [:all]
         clients -> [:all])

  ;; Предыдущий макрос создает эти функции
  (select-operator-proposal) ;; select * proposal;
  (select-operator-clients)  ;; select * from clients;



  (group Director
         proposal -> [:all]
         clients -> [:all]
         agents -> [:all])

  ;; Предыдущий макрос создает эти функции
  (select-director-proposal) ;; select * proposal;
  (select-director-clients)  ;; select * from clients;
  (select-director-agents)  ;; select * from agents;
  

  ;; Определяем пользователей и их группы

  (user Ivanov
        (belongs-to Agent))

  (user Sidorov
        (belongs-to Agent))

  (user Petrov
        (belongs-to Operator))

  (user Directorov
        (belongs-to Operator,
                    Agent,
                    Director))


  ;; Оператор select использует внутри себя переменную <table-name>-fields-var.
  ;; Для указанного юзера макрос with-user должен определять переменную <table-name>-fields-var
  ;; для каждой таблицы, которая должна содержать список допустимых полей этой таблицы
  ;; для этого пользователя.

  ;; Агенту можно видеть свои "предложения"
  (with-user Ivanov
    (select proposal
            (fields :person, :phone, :address, :price)
            (join agents (= agents.proposal_id proposal.id))))

  ;; Агенту не доступны клиенты
  (with-user Ivanov
    (select clients
            (fields :all)))  ;; Empty set

  ;; Директор может видеть состояние задач агентов
  (with-user Directorov
    (select agents
            (fields :done)
            (where {:agent "Ivanov"})
            (order :done :ASC)))
  
  )

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; TBD: Implement the following macros
;;

(defmacro group [name & body]
  ;; Пример
  ;; (group Agent
  ;;      proposal -> [person, phone, address, price]
  ;;      agents -> [clients_id, proposal_id, agent])
  ;; 1) Создает группу Agent
  ;; 2) Запоминает, какие таблицы (и какие колонки в таблицах)
  ;;    разрешены в данной группе.
  ;; 3) Создает следующие функции
  ;;    (select-agent-proposal) ;; select person, phone, address, price from proposal;
  ;;    (select-agent-agents)  ;; select clients_id, proposal_id, agent from agents;
  (let [;; store group name in lower case
        group-name (string/lower-case name)

        ;; list of generated select functions
        select-functions (ref '(do))

        ;; remember group-table-field relationships
        add-table-fields (fn [sym-table arrow sym-fields & other]
                           (let [table (str sym-table)
                                 fields (map keyword sym-fields)]
                             ;;
                             ;; group-table-field stores:
                             ;;   * 1-st level hash map, keys = group name,
                             ;;                          values = 2-nd level hash map
                             ;;   * 2-nd level hash map, keys = table name,
                             ;;                          values = list of columns
                             ;;
                             ;; grout-table-field looks as follows:
                             ;;
                             ;; {"operator" {"clients" (:all), "proposal" (:all)},
                             ;;  "director" {"agents" (:all), "clients" (:all), "proposal" (:all)},
                             ;;  "agent"
                             ;;  {"agents" (:clients_id :proposal_id :agent),
                             ;;   "proposal" (:person :phone :address :price)}}
                             ;;
                             (def group-table-field)
                             (if (bound? #'group-table-field)
                               (def group-table-field (assoc-in group-table-field [group-name table] fields))
                               (def group-table-field {group-name {table fields}}))
                             `(defn ~(symbol (str "select-" group-name "-" table)) []
                                (let [~(symbol (str table "-fields-var")) ~(vec fields)]
                                  (select ~sym-table (~(symbol "fields") ~@fields))))))

        ;; process body by taking three pieces at a time:
        ;; table-name 'arrow field-names
        process-body (fn [body storage]
                       (loop [args body]
                         (when (not (empty? args))
                           (dosync
                            (alter storage
                                   #(cons %2 %1)
                                   (apply add-table-fields args)))
                           (recur (drop 3 args))))
                       (reverse @storage))]
    
    (process-body body select-functions)))

(defmacro user [name & body]
  ;; Пример
  ;; (user Ivanov
  ;;     (belongs-to Agent))
  ;; Создает переменные Ivanov-proposal-fields-var = [:person, :phone, :address, :price]
  ;; и Ivanov-agents-fields-var = [:clients_id, :proposal_id, :agent]
  (let [user-name (string/lower-case name)

        ;; Remember which groups the user belongs to
        ;; I store all the information in one single global variable
        ;; Sorry for neglecting the advice
        handle-belongs-to (fn [groups]
                            (let [group-list (map string/lower-case groups)]
                              ;;
                              ;; user-group is a hash-map of:
                              ;; {user-name:string #{set of groups}}
                              ;;
                              ;; user-group looks as follows:
                              ;;
                              ;; {"sidorov" #{"agent"},
                              ;;  "ivanov" #{"agent"},
                              ;;  "petrov" #{"operator"},
                              ;;  "directorov" #{"operator" "agent" "director"}}
                              ;;
                              (def user-group)
                              (if (bound? #'user-group)
                                (def user-group (assoc-in user-group [user-name] (into #{} group-list)))
                                (def user-group {user-name (into #{} group-list)}))))

        ;; process command. 'belongs-to only is supported for now
        handle-action (fn [command & group-list]
                        (if (= command 'belongs-to)
                          (handle-belongs-to group-list)
                          (println "Unknown action" (str command))))]

    (apply handle-action (first body))
    ;; this macro does not generate any code, this might be a function as well
    ;; so we return empty list (do nothing)
    '()))

(defmacro with-user [name & body]
  ;; Пример
  ;; (with-user Ivanov
  ;;   . . .)
  ;; 1) Находит все переменные, начинающиеся со слова Ivanov, в *user-tables-vars*
  ;;    (Ivanov-proposal-fields-var и Ivanov-agents-fields-var)
  ;; 2) Создает локальные привязки без префикса Ivanov-:
  ;;    proposal-fields-var и agents-fields-var.
  ;;    Таким образом, функция select, вызванная внутри with-user, получает
  ;;    доступ ко всем необходимым переменным вида <table-name>-fields-var.

  (let [user-name (string/lower-case name)

        ;; looks as follows:
        ;; #{"operator" "agent" "director"}
        user-groups (user-group user-name)

        ;; looks as follows:
        ;; ({"clients" (:all), "proposal" (:all)}
        ;;  {"agents" (:clients_id :proposal_id :agent),
        ;;   "proposal" (:person :phone :address :price)}
        ;;  {"agents" (:all), "clients" (:all), "proposal" (:all)})
        user-tables (map #(group-table-field %) user-groups)

        ;; looks as follows:
        ;; {"agents" #{:proposal_id :clients_id :agent :all},
        ;;  "proposal" #{:phone :person :all :price :address},
        ;;  "clients" #{:all}}
        table-keys (reduce (fn [acc x]
                             (reduce (fn [acc-in x-in]
                                       (update-in acc-in
                                                  [(first x-in)]
                                                  clojure.set/union
                                                  (into #{} (second x-in))))
                                     acc
                                     x))
                           {}
                           user-tables)
        generate-let (fn [[key value]]
                       `(~(symbol (str key "-fields-var")) ~(vec value)))]

    `(let ~(vec (mapcat generate-let table-keys))
      ~@body)))