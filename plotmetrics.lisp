;;;; Copyright (C) 2022 Cosimo Agati

;;;; This program is free software: you can redistribute it and/or
;;;; modify it under the terms of the GNU Affero General Public License
;;;; as published by the Free Software Foundation, either version 3 of
;;;; the License, or (at your option) any later version.

;;;; You should have received a copy of the GNU AGPLv3 with this software,
;;;; if not, please visit <https://www.gnu.org/licenses/>

(declaim (optimize (speed 3)))

(eval-when (:compile-toplevel :load-toplevel)
  (ql:quickload '(:vgplot :yason :alexandria :cl-ppcre :py4cl))
  (py4cl:import-module "matplotlib.pyplot" :as "plt"))

(defpackage plotmetrics
  (:use :common-lisp)
  (:export :plot :make-parameters :generate-images :*debug*))

(in-package plotmetrics)

(defvar *debug* t)

(defclass plot-parameters ()
  ((batch-sizes :type list :accessor batch-sizes  :initarg :batch-sizes
                :initform (list 0 1 2 4 8 16 32 64 128))
   (single-batch-size :type fixnum :accessor single-batch-size
                      :initarg :single-batch-size :initform 0)
   (parallelism-degrees :type list :accessor pardegs :initarg :pardegs
                        :initform (list 1 5 10))
   (single-parallelism-degree :type fixnum :accessor single-pardeg
                              :initarg :single-pardeg :initform 1)
   (sampling-rate :type fixnum :accessor sampling-rate :initarg :sampling-rate
                  :initform 100)
   (tuple-generation-rate :type fixnum :accessor tuple-rate
                          :initarg :tuple-rate :initform 0)
   (chaining-p :type boolean :accessor chaining-p :initarg :chaining-p
               :initform nil)
   (execution-mode :type string :accessor execmode :initarg :execmode
                   :initform "default")
   (plot-by :type symbol :accessor plot-by :initarg :plot-by
            :initform :parallelism)
   (metric-to-plot :type string :accessor metric :initarg :metric
                   :initform "throughput")
   (percentile :type string :accessor percentile :initarg :percentile
               :initform "mean")
   (percentiles :type list :accessor percentiles :initarg :percentiles
                :initform (list 0 5 25 50 75 95 100))
   (compare-by :type symbol :accessor compare-by :initarg :compare-by
               :initform :batch-size)
   (timer-nodes-p :type boolean :accessor timer-nodes-p :initarg :timer-nodes-p
                  :initform t)
   (frequencies :type list :accessor frequencies :initarg :frequencies
                :initform (list 2 4 6 8 10))
   (single-frequency :type fixnum :accessor single-freq :initarg :single-freq
                     :initform 2)
   (directory-to-plot :type (or string pathname) :accessor plotdir
                      :initarg :plotdir :initform "")
   (plot-kind :type symbol :accessor plot-kind :initarg :plot-kind
              :initform :normal)))

(defun make-parameters (&rest args)
  (apply #'make-instance 'plot-parameters args))

(defvar *default-plot-parameters* (make-parameters))

(defun set-plot-by (metric)
  (unless (member metric '(:parallelism :batch-size :chaining))
    (error "Metric must be one of :PARALLELISM, :BATCH-SIZE or :CHAINING"))
  (setf (plot-by *default-plot-parameters*) metric))

(defun set-compare-by (metric)
  (unless (member metric '(:parallelism :batch-size :chaining :execmode))
    (error "Metric must be one of :PARALLELISM, :BATCH-SIZE or :CHAINING"))
  (setf (compare-by *default-plot-parameters*) metric))

(defun set-batch-sizes (batch-sizes)
  (when (some (lambda (b) (not (member b '(0 1 2 4 8 16 32 64 128))))
              batch-sizes)
    (error "Batch size must be either one of 0 1 2 4 8 16 32 64 128"))
  (setf (batch-sizes *default-plot-parameters*) batch-sizes))

(defun set-parallelism-degrees (parallelism-degrees)
  (setf (pardegs *default-plot-parameters*) parallelism-degrees))

(defun set-single-parallelism-degree (parallelism-degree)
  (setf (single-pardeg *default-plot-parameters*) parallelism-degree))

(defun set-sampling-rate (rate)
  (unless (and (integerp rate) (not (minusp rate)))
    (error "Sampling rate must be a non-negative integer"))
  (setf (sampling-rate *default-plot-parameters*) rate))

(defun set-tuple-generation-rate (rate)
  (unless (and (integerp rate) (not (minusp rate)))
    (error "Tuple generation rate must be a non-negative integer"))
  (setf (tuple-rate *default-plot-parameters*) rate))

(defun set-chaining (chaining)
  (setf (chaining-p *default-plot-parameters*) chaining))

(defun set-metric-to-plot (metric)
  (unless (member metric '(:throughput :service-time :latency
                           :total-latency :geo-latency
                           :volume-latency :status-latency))
    (error "Invalid metric"))
  (setf (metric *default-plot-parameters*)
        (string-downcase (symbol-name metric))))

(defun set-percentile (percentile)
  (setf (percentile *default-plot-parameters*) percentile))

(defun set-directory-to-plot (directory)
  (unless (or (pathnamep directory) (stringp directory))
    (error "DIRECTORY must be a pathname or a string designating a path"))
  (setf (plotdir *default-plot-parameters*) directory))

(defun concat (&rest strings)
  (declare (list strings))
  (apply #'concatenate 'string strings))

(let ((unit-abbrevs '(("microseconds" . "us")
                      ("microsecond" . "us")
                      ("us" . "us")
                      ("nanoseconds" . "ns")
                      ("nanosecond" . "ns")
                      ("ns" . "ns")
                      ("milliseconds" . "ms")
                      ("millisecond" . "ms")
                      ("ms" . "ms")
                      ("seconds" . "s")
                      ("second" . "s")
                      ("s" . "s"))))
  (defun unit-to-abbrev (unit)
    "Return the time unit abbreviation for UNIT."
    (declare (string unit))
    (cdr (assoc unit unit-abbrevs :test #'string=))))

(let ((scale-factors '(("ns" . 1000000000)
                       ("us" . 1000000)
                       ("ms" . 1000)
                       ("s" . 1))))
  (defun time-unit-scale-factor (time-unit)
    "Return how many of the specified TIME-UNIT are required to form a second."
    (declare ((or symbol string) time-unit))
    (let ((key (if (symbolp time-unit)
                   (string-downcase (symbol-name time-unit))
                   time-unit)))
      (cdr (assoc key scale-factors :test #'equal)))))

(defun base-dirname (directory)
  (second (pathname-directory directory)))

(defun title-from-directory (directory)
  (let* ((words (cl-ppcre:split "-" (base-dirname directory)))
         (acronym (first words))
         (title (concat (second words) " " (third words))))
    (concat (string-capitalize title) " (" (string-upcase acronym) ")")))

(defun ends-with (word suffix)
  "Return non-NIL if WORD ends with SUFFIX, NIL otherwise."
  (declare (string word suffix))
  (alexandria:ends-with-subseq suffix word :test #'equal))

(defun get-json-objs-from-directory (directory)
  "Return a list containing containing pathnames to JSON files in DIRECTORY."
  (let ((file-list (directory (merge-pathnames directory "*"))))
    (mapcar #'yason:parse
            (delete-if-not (lambda (f) (ends-with (namestring f) ".json"))
                           file-list))))

(Defun json-name-match (entry name)
  "Return non-NIL if ENTRY ends with NAME (with some transformations.
Otherwise, return NIL."
  (declare (string entry) (string name))
  (or (ends-with entry name)
      (ends-with (substitute #\Space #\- entry) name)
      (ends-with (substitute #\Space #\- entry) name)))

(defun filter-jsons-by-name (jsons name)
  "Return a list containing only the json files whose \"name\" field is NAME"
  (declare (list jsons) (string name))
  (remove-if-not (lambda (j) (json-name-match (gethash "name" j) name))
                 jsons))

(defun filter-jsons-by-parallelism (jsons parallelism)
  "Filter out entries in JSONS not matching PARALLELISM.
Return a brand new list, the original list is left untouched."
  (declare (list jsons) (fixnum parallelism))
  (remove-if-not (lambda (j) (= (first (gethash "parallelism" j)) parallelism))
                 jsons))

(defun filter-jsons-by-batch-size (jsons batch-size)
  "Filter out entries in JSONS not matching BATCH-SIZE.
Return a brand new list, the original list is left untouched."
  (declare (list jsons) (fixnum batch-size))
  (remove-if-not (lambda (j) (= (first (gethash "batch size" j)) batch-size))
                 jsons))

(defun filter-jsons-by-chaining (jsons chaining-p)
  "Filter out entries in JSONS not matching CHAINING-P.
Return a brand new list, the original list is left untouched."
  (declare (list jsons) (boolean chaining-p))
  (remove-if-not (lambda (j) (eql (gethash "chaining enabled" j) chaining-p))
                 jsons))

(defun get-sampling-rate (json)
  "Extract tuple sampling rate from the specified JSON object."
  (declare (hash-table json))
  (multiple-value-bind (val1 present1-p) (gethash "sampling_rate" json)
    (multiple-value-bind (val2 present2-p) (gethash "sampling rate" json)
      (if present1-p (values val1 present1-p) (values val2 present2-p)))))

(defun filter-jsons-by-sampling-rate (jsons sampling-rate)
  "Filter out entries in JSONS not matching SAMPLING-RATE.
Return a brand new list, the original list is left untouched."
  (declare (list jsons) (fixnum sampling-rate))
  (remove-if-not (lambda (j) (= (get-sampling-rate j) sampling-rate))
                 jsons))

(defun contains-frequency-fields-p (json)
  (declare (hash-table json))
  (let ((keys (alexandria:hash-table-keys json)))
    (some (lambda (k) (ends-with k "frequency")) keys)))

(defun frequency-fields-equal-value-p (json value)
  (declare (hash-table json) (fixnum value))
  (let ((frequency-keys (remove-if-not (lambda (k) (ends-with k "frequency"))
                                       (alexandria:hash-table-keys json))))
    (every (lambda (v) (equal v value))
           (mapcar (lambda (k) (gethash k json)) frequency-keys))))

(defun filter-jsons-by-frequency (jsons frequency)
  (declare (list jsons) (fixnum frequency))
  (remove-if-not (lambda (j) (frequency-fields-equal-value-p j frequency))
                 jsons))

(defun contains-timer-node-key (json)
  (declare (hash-table json))
  (let ((keys (alexandria:hash-table-keys json)))
    (member "using timer nodes" keys :test #'equal)))

(defun filter-jsons-by-timernode-impl (jsons timer-nodes-p)
  (declare (list jsons) (boolean timer-nodes-p))
  (remove-if-not (lambda (j)
                   (eql (gethash "using timer nodes" j) timer-nodes-p))
                 jsons))

(defun filter-jsons (parameters jsons)
  (declare (plot-parameters parameters) (list jsons))
  (with-accessors ((plot-by plot-by) (compare-by compare-by))
      parameters
    (setf jsons (filter-jsons-by-name jsons (metric parameters))
          jsons (filter-jsons-by-sampling-rate jsons (sampling-rate parameters))
          jsons (filter-jsons-by-tuple-rate jsons (tuple-rate parameters)))
    (unless (or (eql plot-by :parallelism) (eql compare-by :parallelism))
      (setf jsons (filter-jsons-by-parallelism jsons
                                               (single-pardeg parameters))))
    (unless (or (eql plot-by :batch-size) (eql compare-by :batch-size))
      (setf jsons (filter-jsons-by-batch-size jsons
                                              (single-batch-size parameters))))
    (unless (or (eql plot-by :chaining) (eql compare-by :chaining))
      (setf jsons (filter-jsons-by-chaining jsons (chaining-p parameters))))
    (when (and (some #'contains-frequency-fields-p jsons)
               (not (eql compare-by :frequency)))
      (setf jsons (filter-jsons-by-frequency jsons (single-freq parameters))))
    (unless (eql compare-by :execmode)
      (setf jsons (filter-jsons-by-execmode jsons (execmode parameters))))
    (when (and (some #'contains-timer-node-key jsons)
               (not (eql compare-by :timernode-impl)))
      (setf jsons (filter-jsons-by-timernode-impl jsons
                                                  (timer-nodes-p parameters))))
    (setf jsons (ecase plot-by
                  (:parallelism (sort-jsons-by-parallelism jsons))
                  (:batch-size (sort-jsons-by-batch-size jsons))
                  (:chaining (sort-jsons-by-chaining jsons))))
    jsons))

(defun get-tuple-rate (json)
  "Extract tuple generation rate from the specified JSON object."
  (declare (hash-table json))
  (multiple-value-bind (val1 present1-p) (gethash "tuple_rate" json)
    (multiple-value-bind (val2 present2-p) (gethash "tuple rate" json)
      (if present1-p val1 (values val2 present2-p)))))

(defun filter-jsons-by-tuple-rate (jsons tuple-rate)
  "Filter out entries in JSONS not matching TUPLE-RATE.
Return a brand new list, the original list is left untouched."
  (declare (list jsons) (fixnum tuple-rate))
  (remove-if-not (lambda (j) (= (get-tuple-rate j) tuple-rate))
                 jsons))

(defun filter-jsons-by-execmode (jsons execmode)
  "Filter out entries in JSONS not matching EXECMODE.
Return a brand new list, the original list is left untouched."
  (declare (list jsons) (string execmode))
  (remove-if-not (lambda (j) (string= (gethash "execution mode" j) execmode))
                 jsons))

(defun percentile-to-dictkey (kind)
  "From KIND, return the right string representing the key to index JSONs."
  (declare (string kind))
  (cond ((member kind '("0th" "5th" "25th" "50th" "75th" "95th" "100th")
                 :test #'equal)
         (concat kind " percentile"))
        ((member kind '("0" "5" "25" "50" "75" "95" "100")
                 :test #'equal)
         (concat kind "th percentile"))
        ((member kind '(0 5 25 50 75 95 100))
         (concat (write-to-string kind) "th percentile"))
        (t kind)))

(defun get-x-label (plot-by)
  (declare (symbol plot-by))
  (ecase plot-by
    (:parallelism "Parallelism degree for each node")
    (:batch-size "Initial batch size")
    (:chaining "Chaining enabled?")))

(defun get-y-label (name time-unit)
  "Return the proper Y label name from NAME and TIME-UNIT."
  (declare (string name time-unit))
  (let* ((time-unit-string (let ((abbrev (unit-to-abbrev time-unit)))
                             (if abbrev abbrev "unknown unit")))
         (unit-string (if (not (search "throughput" (string-downcase name)))
                          time-unit-string
                          "tuples per second")))
    (concat (string-capitalize (substitute #\Space #\- name))
            " (" unit-string ")")))

(defun scale-by-base-value (base-value y measure)
  (declare (real base-value y) (string measure))
  (if (search "throughput" measure) (/ y base-value) (/ base-value y)))

(defun get-scaled-y-axis (name jsons percentile base-value)
  (declare (string name percentile) (list jsons) (real base-value))
  (let ((y-axis (mapcar (lambda (j) (gethash percentile j))
                        jsons)))
    (declare (list y-axis))
    (map-into y-axis (lambda (y) (scale-by-base-value base-value y name))
              y-axis)))

(defun get-efficiency-y-axis (name jsons percentile base-value)
  (declare (string name percentile) (list jsons) (real base-value))
  (let ((scaled-y-axis (get-scaled-y-axis name jsons
                                          percentile base-value)))
    (declare (list scaled-y-axis))
    (do ((y-axis scaled-y-axis (rest y-axis))
         (i 1 (1+ i)))
        ((null y-axis) scaled-y-axis)
      (setf (car y-axis) (/ (car y-axis) i)))))

(defun get-y-axis (name jsons percentile plot-kind)
  (declare (string name percentile) (list jsons) (symbol plot-kind))
  (ecase plot-kind
    (:normal
     (let* ((time-unit (gethash "time unit" (first jsons)))
            (map-func
              (if (not (search name "throughput"))
                  (lambda (j)
                    (gethash (percentile-to-dictkey percentile) j))
                  (lambda (j)
                    (* (time-unit-scale-factor (unit-to-abbrev time-unit))
                       (gethash (percentile-to-dictkey percentile) j))))))
       (declare (function map-func))
       (mapcar map-func jsons)))
    (:scalability
     (let ((base-value (gethash (percentile-to-dictkey percentile)
                                (first jsons))))
       (get-scaled-y-axis name jsons percentile base-value)))
    (:efficiency
     (let ((base-value (gethash (percentile-to-dictkey percentile)
                                (first jsons))))
       (get-efficiency-y-axis name jsons percentile base-value)))))

(defun get-percentile-values (percentile-map percentile-keys)
  (declare (hash-table percentile-map) (list percentile-keys))
  (mapcar (lambda (p) (let ((key (concat (write-to-string p) "th percentile")))
                        (gethash key percentile-map)))
          percentile-keys))

(defun get-x-axis (plot-by jsons)
  (declare (symbol plot-by) (list jsons))
  (ecase plot-by
    (:parallelism (mapcar (lambda (j)
                            (declare (hash-table j))
                            (first (gethash "parallelism" j)))
                          jsons))
    (:batch-size (mapcar (lambda (j)
                           (declare (hash-table j))
                           (first (gethash "batch size" j)))
                         jsons))
    (:chaining (mapcar (lambda (x)
                         (declare (boolean x))
                         (if x 1 0))
                       (mapcar (lambda (j)
                                 (declare (hash-table j))
                                 (gethash "chaining" j))
                               jsons)))))

(defun chaining-to-string (chaining-p)
  (declare (boolean chaining-p))
  (if chaining-p "enabled" "disabled"))

(defun tuple-rate-to-string (rate)
  (declare (fixnum rate))
  (if (plusp rate) (write-to-string rate) "unlimited"))

(defun title-for-plot (parameters)
  (declare (plot-parameters parameters))
  (let ((initial-title (concat (title-from-directory (plotdir parameters))
                               " - " (substitute #\Space #\-
                                                 (string-capitalize
                                                  (metric parameters)))
                               " (" (percentile parameters) ") ")))
    (declare (string initial-title))
    (title-from-plotby-compareby initial-title parameters (plot-by parameters)
                                 (compare-by parameters))))

(symbol-macrolet ((chaining (chaining-to-string (chaining-p parameters)))
                  (pardeg (write-to-string (single-pardeg parameters)))
                  (batch-size (write-to-string
                               (single-batch-size parameters)))
                  (tuple-rate (tuple-rate-to-string
                               (tuple-rate parameters))))
  (defgeneric title-from-plotby-compareby (title parameters plot-by compare-by)
    (:method (title parameters (plot-by (eql :parallelism))
              (compare-by (eql :batch-size)))
      (concat title "(chaining: " chaining ") (generation rate: "
              tuple-rate ")"))
    (:method (title parameters (plot-by (eql :parallelism))
              (compare-by (eql :chaining)))
      (concat title "(batch size: " batch-size ") (generation rate: "
              tuple-rate ")"))
    (:method (title parameters (plot-by (eql :parallelism))
              (compare-by (eql :execmode)))
      (concat title "(chaining " chaining ") (generation rate: " tuple-rate
              ") (batch size: " batch-size ")"))
    (:method (title parameters (plot-by (eql :parallelism))
              (compare-by (eql :frequency)))
      (concat title "(chaining: " chaining ") (generation rate: " tuple-rate
              ") (batch-size: " batch-size))
    (:method (title parameters (plot-by (eql :batch-size))
              (compare-by (eql :parallelism)))
      (concat title "(chaining " chaining ") (generation rate: "
              tuple-rate ")"))
    (:method (title parameters (plot-by (eql :batch-size))
              (compare-by (eql :chaining)))
      (concat title " (parallelism degree per node: " pardeg
              ") (generation rate: " tuple-rate ")"))
    (:method (title parameters (plot-by (eql :batch-size))
              (compare-by (eql :execmode)))
      (concat title " (parallelism degree per node: " pardeg ") (chaining: "
              chaining ") (generation rate: " tuple-rate ")"))
    (:method (title parameters (plot-by (eql :chaining))
              (compare-by (eql :parallelism)))
      (concat title " (batch size : " batch-size
              ") (generation rate: " tuple-rate ")"))
    (:method (title parameters (plot-by (eql :chaining))
              (compare-by (eql :batch-size)))
      (concat title " (parallelism degree per node: " pardeg
              ") (generation rate: " tuple-rate ")"))))

(defun sort-jsons-by-parallelism (jsons)
  (declare (list jsons))
  (sort jsons #'< :key (lambda (j) (first (gethash "parallelism" j)))))

(defun sort-jsons-by-batch-size (jsons)
  (declare (list jsons))
  (sort jsons #'< :key (lambda (j) (first (gethash "batch size" j)))))

(defun sort-jsons-by-chaining (jsons)
  (declare (list jsons))
  (sort jsons (lambda (a b) (and a (not b)))
        :key (lambda (j) (gethash "chaining" j))))

(defun plot-from-triples (triples)
  (declare (list triples))
  (apply #'vgplot:plot triples))

(defun save-plot (name)
  (declare ((or string pathname) name))
  (vgplot:print-plot (pathname name)
                     :terminal "png size 1280,960"))

(defun draw-boxplot (&key title x-label y-label y-axis x-axis)
  (declare (string title x-label y-label)
           (list x-axis y-axis))
  ;; (py4cl:import-module "matplotlib.pyplot" :as "plt")
  (plt:figure)
  (plt:xlabel x-label)
  (plt:ylabel y-label)
  (plt:title title :loc "right" :y 1.00)
  (plt:grid t)
  (plt:boxplot y-axis :positions x-axis)
  (plt:show)
  (plt:close "all"))

(defun boxplot-title (parameters)
  (declare (plot-parameters parameters))
  (with-accessors ((plotdir plotdir) (metric metric)
                   (batch-size single-batch-size)
                   (chaining-p chaining-p) (percentiles percentiles))
      parameters
    (concat (title-from-directory plotdir) " - "
            (substitute #\Space #\- (string-capitalize metric))
            " (batch size: " (write-to-string batch-size)
            ") (chaining: " (if chaining-p "enabled " "disabled")
            ") (percentiles: " (write-to-string percentiles) ")")))

(defun get-plot-triples (parameters jsons comparison-values filter-func
                         label-func)
  (declare (plot-parameters parameters) (list jsons comparison-values)
           (function filter-func label-func))
  (let (plot-triples)
    (dolist (comparison-value comparison-values plot-triples)
      (let ((current-jsons (funcall filter-func jsons comparison-value)))
        (when current-jsons
          (let ((x-axis (get-x-axis (plot-by parameters) current-jsons))
                (y-axis (get-y-axis (metric parameters) current-jsons
                                    (percentile parameters)
                                    (plot-kind parameters)))
                (label (funcall label-func comparison-value)))
            (push label plot-triples)
            (push y-axis plot-triples)
            (push x-axis plot-triples)))))))

(defun get-triples-comparing-by (parameters jsons compare-by)
  (declare (plot-parameters parameters) (list jsons) (symbol compare-by))
  (ecase compare-by
    (:parallelism
     (get-plot-triples parameters jsons (pardegs parameters)
                       #'filter-jsons-by-parallelism
                       (lambda (value)
                         (concat "Parallelism degree: "
                                 (write-to-string value)))))
    (:batch-size
     (get-plot-triples parameters jsons (batch-sizes parameters)
                       #'filter-jsons-by-batch-size
                       (lambda (value)
                         (concat "Batch size: " (write-to-string value)))))
    (:chaining
     (get-plot-triples parameters jsons '(nil t)
                       #'filter-jsons-by-chaining
                       (lambda (value)
                         (concat "Chaining: " (chaining-to-string value)))))
    (:execmode
     (get-plot-triples parameters jsons '("deterministic" "default")
                       #'filter-jsons-by-execmode
                       (lambda (value) (concat "Execution mode: " value))))
    (:frequency
     (get-plot-triples parameters jsons (frequencies parameters)
                       #'filter-jsons-by-frequency
                       (lambda (value)
                         (concat "Output frequency for all operators: "
                                 (write-to-string value)))))))

(defun get-additional-triples (length plot-kind)
  (ecase plot-kind
    (:scalability (let* ((x-axis (loop for i from 1 to length collect i))
                         (y-axis x-axis)
                         (label "Ideal scalability"))
                    (values x-axis y-axis label)))
    (:efficiency (let ((x-axis (loop for i from 1 to length collect i))
                       (y-axis (loop repeat length collect 1))
                       (label "Ideal efficiency"))
                   (values x-axis y-axis label)))))

(defun get-triples (parameters jsons)
  (declare (plot-parameters parameters) (list jsons))
  (let ((actual-triples (get-triples-comparing-by parameters jsons
                                                  (compare-by parameters))))
    (if (eql (plot-kind parameters) :normal)
        actual-triples
        (let ((length (length (second actual-triples))))
          (multiple-value-bind (x-axis y-axis label)
              (get-additional-triples length (plot-kind parameters))
            (nconc (list x-axis y-axis label) actual-triples))))))

(defun plot (&optional (parameters *default-plot-parameters*) jsons
               image-path)
  (declare (plot-parameters parameters) (list jsons)
           ((or pathname string) image-path))
  (unless jsons
    (setf jsons (get-json-objs-from-directory (plotdir parameters))))
  (setf jsons (filter-jsons parameters jsons))
  (unless jsons
    (format t "No data found with the specified parameters, not plotting...")
    (return-from plot))
  (let ((time-unit (gethash "time unit" (first jsons))))
    (let ((title (title-for-plot parameters))
          (xlabel (get-x-label (plot-by parameters)))
          (ylabel (get-y-label (metric parameters) time-unit)))
      (vgplot:title title)
      (vgplot:xlabel xlabel)
      (vgplot:ylabel ylabel)
      (vgplot:grid t)
      (let ((plot-triples (get-triples parameters jsons)))
        (when *debug*
          (print (reverse plot-triples)))
        (plot-from-triples plot-triples))
      (when image-path
        (save-plot image-path)))))

(defun boxplot (&optional (parameters *default-plot-parameters*) jsons
                  image-path)
  (declare (plot-parameters parameters) (list jsons)
           ((or pathname string) image-path) )
  (unless jsons
    (setf jsons (get-json-objs-from-directory (plotdir parameters))))
  (setf jsons (filter-jsons-by-name jsons (metric parameters))
        jsons (filter-jsons-by-chaining jsons (chaining-p parameters))
        jsons (filter-jsons-by-batch-size jsons (single-batch-size parameters))
        jsons (filter-jsons-by-sampling-rate jsons (sampling-rate parameters))
        jsons (filter-jsons-by-tuple-rate jsons (tuple-rate parameters)))
  (unless jsons
    (format t "No data found with the specified parameters, not plotting...")
    (return-from boxplot))
  (setf jsons (sort-jsons-by-parallelism jsons))
  (let ((time-unit (gethash "time unit" (first jsons))))
    (let ((title (boxplot-title parameters))
          (xlabel (get-x-label (plot-by parameters)))
          (ylabel (get-y-label (metric parameters) time-unit))
          (x-axis (get-x-axis (plot-by parameters) jsons))
          (y-axis (mapcar (lambda (j)
                            (get-percentile-values j (percentiles parameters)))
                          jsons)))
      (when *debug*
        (format t "x-axis: ~a~%y-axis: ~a~%" x-axis y-axis))
      (draw-boxplot :title title :x-label xlabel :y-label ylabel
                    :x-axis x-axis :y-axis y-axis))))

(defun get-image-file-name (parameters)
  (declare (plot-parameters parameters))
  (let ((output-file-name
          (concat (metric parameters) "-"
                  (string-downcase (symbol-name (plot-kind parameters)))
                  "-by-" (string-downcase (symbol-name (plot-by parameters)))
                  "-compare-by-"
                  (string-downcase
                   (symbol-name (compare-by parameters)))
                  (if (eql (compare-by parameters) :batch-size)
                      (concat "-chaining-"
                              (chaining-to-string (chaining-p parameters)))
                      "")
                  ".png")))
    (pathname (concat (namestring (plotdir parameters))
                      "/graphs/" output-file-name))))

(defun generate-images (&optional (parameters *default-plot-parameters*))
  (declare (plot-parameters parameters))
  (with-accessors ((plot-by plot-by) (plotdir plotdir)
                   (compare-by compare-by) (plot-kind plot-kind))
      parameters
    (let ((jsons (get-json-objs-from-directory plotdir)))
      (dolist (current-plot-by '(:parallelism :batch-size :chaining))
        (setf plot-by current-plot-by)
        (dolist (current-compare-by (remove current-plot-by '(:parallelism
                                                              :batch-size
                                                              :chaining)))
          (when *debug*
            (print (list current-plot-by current-compare-by)))
          (setf compare-by current-compare-by)
          (dolist (metric '("throughput" "service time" "latency"))
            (setf (metric parameters) metric)
            (dolist (plot-kind '(:normal :scalability :efficiency))
              (setf (plot-kind parameters) plot-kind)
              (unless (eql current-plot-by :chaining)
                (dolist (chaining-p '(nil t))
                  (setf (chaining-p parameters) chaining-p)
                  (let ((image-path (get-image-file-name parameters)))
                    (plot parameters jsons image-path)))))))))))
