(ns app.core-test
  (:require [app.core :as sut]
            [clojure.test :refer [testing is deftest]]))

(defn servers-range [servers-total]
  (range 1 (inc servers-total)))

(deftest servers-in-order-test
  (testing "Should return 1 to n seq, with empty last-picked-server"
    (let [servers-total 3]
      (is (= (servers-range servers-total)
             (sut/servers-in-order servers-total {}))))
    (let [servers-total 6]
      (is (= (servers-range servers-total)
             (sut/servers-in-order servers-total {})))))
  
  (testing "If last-picked-server is last server, seq should start from first server"
    (let [servers-total      4
          last-picked-server 4
          next-server        1]
      (is (= next-server
             (first
              (sut/servers-in-order servers-total
                                    {:last-picked-server last-picked-server})))))
    (let [servers-total      3
          last-picked-server 3
          next-server        1]
      (is (= next-server
             (first
              (sut/servers-in-order servers-total
                                    {:last-picked-server last-picked-server}))))))
  
  (testing "First server should be next to last-picked-server"
    (let [servers-total      4
          last-picked-server 3
          next-server        4]
      (is (= next-server
             (first
              (sut/servers-in-order servers-total
                                    {:last-picked-server last-picked-server})))))
    (let [servers-total      3
          last-picked-server 3
          next-server        1]
      (is (= next-server
             (first
              (sut/servers-in-order servers-total
                                    {:last-picked-server last-picked-server})))))))


(deftest next-idle-server-test
  (testing "If state is empty should return server 1"
    (let [servers-total 3
          state         {}
          current-time  3]
      (is (= 1
             (sut/next-idle-server servers-total state current-time)))))
  
  (testing "Should return next to last-picked-server if it is idle"
    (let [servers-total 3
          state         {:last-picked-server 1}
          current-time  3]
      (is (= 2
             (sut/next-idle-server servers-total state current-time))))
    (let [servers-total 3
          state         {:last-picked-server 3}
          current-time  3]
      (is (= 1
             (sut/next-idle-server servers-total state current-time)))))
  
  (testing "Should return next to last-picked-server, if his last req is finished to current-time"
    (let [servers-total 3
          state         {2                   [[1 4]]
                         :last-picked-server 1}
          current-time  4]
      (is (= 2
             (sut/next-idle-server servers-total state current-time)))))
  
  (testing "Should not return next to last-picked-server if it is busy"
    (let [servers-total 3
          state         {2                   [[1 4]]
                         :last-picked-server 1}
          current-time  3]
      (is (not= 2
                (sut/next-idle-server servers-total state current-time)))))
  
  (testing "Should return nil if all servers are busy at current-time"
    (let [servers-total 3
          state         {1                   [[2 3]]
                         2                   [[1 4]]
                         3                   [[1 5]]
                         :last-picked-server 1}
          current-time  3]
      (is (nil? (sut/next-idle-server servers-total state current-time)))))
  
  (testing "Should return idle server if all other servers are busy at current-time"
    (let [servers-total 3
          state         {1                   [[2 2]]
                         2                   [[1 4]]
                         3                   [[1 5]]
                         :last-picked-server 1}
          current-time  3]
      (is (= 1 (sut/next-idle-server servers-total state current-time))))))

(deftest server-process-req-test
  (testing "last-picked-server should be same as server-id"
    (let [state     {}
          server-id 3
          req       [1 5]
          state'    {:last-picked-server 3}]
      (is (= server-id
             (:last-picked-server (sut/server-process-req state server-id req))))))
  
  (testing "Should add req as last server-id list"
    (let [state     {}
          server-id 1
          req       [1 5]
          state'    {:last-picked-server 1}]
      (is (= req
             (last(get (sut/server-process-req state server-id req) server-id)))))
    (let [server-id 1
          state     {server-id [[1 3]]}
          req       [4 5]
          state'    {:last-picked-server 1}]
      (is (= req
             (last(get (sut/server-process-req state server-id req) server-id)))))))

(deftest process-requests-test
  (testing "Each server should processed 1 req"
    (let [servers-max      3
          requests         [[1 1] [1 1] [1 1]]
          [req1 req2 req3] requests
          state            (sut/process-requests servers-max requests)]
      (is (identical? req1 (first (get state 1))))
      (is (identical? req2 (first (get state 2))))
      (is (identical? req3 (first (get state 3))))))
  
  (let [servers-max           3
        requests              [[1 4] [1 2] [1 3] [2 1]]
        [req1 req2 req3 req4] requests
        state                 (sut/process-requests servers-max requests)]
    
    (testing "Req 2 and 4 should processed by server 2"
      (let [[r2 r4] (get state 2)]
        (is (identical? req2 r2))
        (is (identical? req4 r4))))
    
    (testing "Req 1 should processed by server 1"
      (let [[r1] (get state 1)]
        (is (identical? req1 r1))))
    
    (testing "Req 3 should processed by server 3"
      (let [[r3] (get state 3)]
        (is (identical? req3 r3))))))

(defn calc-loads [requests]
  (reduce + 0 (map second requests)))

(deftest calc-loads-test
  (let [stats    {1 [[1 1] [1 1]]
                  2 [[1 2] [1 2]]
                  3 [[1 3]]}
        loads    (sut/calc-loads stats)
        s1-loads (calc-loads (get stats 1))
        s2-loads (calc-loads (get stats 2))
        s3-loads (calc-loads (get stats 3))]
    (testing "Should calc loads for each key"
      (is (= s1-loads (get loads 1)))
      (is (= s2-loads (get loads 2)))
      (is (= s3-loads (get loads 3))))))

(deftest find-top-loads-test
  (let [stats {1 3
               2 5
               3 5}
        top   (set (sut/find-top-loads stats))]
    (testing "Should return keys with 5"
      (is (contains? top 2))
      (is (contains? top 3)))
    
    (testing "Should not return key 1"
      (is (not (contains? top 1))))))
