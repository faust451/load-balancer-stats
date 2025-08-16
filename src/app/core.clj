(ns app.core)

(defn servers-in-order [servers-total {last-picked-server :last-picked-server}]
  (let [last-picked-server (or last-picked-server 0)
        servers            (cycle (range 1 (inc servers-total)))]
    (->> servers
      (drop last-picked-server)
      (take servers-total))))

(defn next-idle-server [servers-total state current-time]
  (let [servers (servers-in-order servers-total state)
        pred    (fn [server-id]
                  (let [loads              (get state server-id)
                        [time-stared load] (or (last loads) [0 0])
                        finish-time        (- (+ time-stared load) 1)]
                    (<= finish-time current-time)))]
    (first (filter pred servers))))

(defn server-process-req [state server-id req]
  (-> state
    (update server-id vec)
    (update server-id conj req)
    (assoc :last-picked-server server-id)))

(defn order-by-arrive-time [arrival]
  (sort-by second arrival))

(defn process-requests [servers-max requests]
  (reduce
   (fn [state [arrival load :as req]]
     (let [idle-server (next-idle-server servers-max state arrival)]
       (if idle-server
         (server-process-req state idle-server req)
         state)))
   {} requests))

(defn sum-requests-loads [requests]
  (->> requests
    (map last)
    (reduce + 0)))

(defn calc-loads [state]
  (let [state (dissoc state :last-picked-server)]
    (reduce
     (fn [acc [server-id requests]]
       (assoc acc server-id (sum-requests-loads requests)))
     {} state)))

(defn find-top-loads [stats]
  (let [stats         (reverse (sort-by second stats))
        [top1 & rest] stats]
    (if top1
      (map first
           (cons top1
                 (filter #(= (second top1) (second %)) rest))))))

(defn run [servers-max arrival load]
  (let [requests (map vector arrival load)
        requests (order-by-arrive-time requests)
        state    (process-requests servers-max requests)]
    (-> state calc-loads find-top-loads)))
