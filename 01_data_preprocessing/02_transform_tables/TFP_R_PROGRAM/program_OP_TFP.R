# function to check and change input to matrix #
checkM <- function(input){ # , inputname = NA
  if (!is.matrix(input)) {
    out <- as.matrix(input)
  } else{
    out <- input
  }
  colnames(out) <- NULL
  return(out)
}
# end check matrix fun #

# function to lag variables within a panel #
lagPanel <- function( idvar, timevar, value, suffix = '' ){
  df <- data.frame( idvar, timevar, value)
  last.time <- df %>% filter(!is.na(timevar)) %>%
    mutate(timevar = timevar + 1, lagged_value = value, value = NULL)
  out <- as.matrix(df %>% left_join(last.time, by = c("idvar", "timevar")))[,4]
  colnames( out ) <- NULL
  return( out )
}
# end of lag panel #

# boot resampling on IDs: bootstrapping on individuals #
block.boot.resample <- function( idvar, R ){
  unique.ids <- unique(idvar) # find the unique values of panels in order to reshape the data
  panel.time.indices <- apply(unique.ids, 1, function(x) {return(list(which(idvar == x)))}) # find the time indices for each panel
  seq.indices <- 1:length(unique.ids) # the panel.time.indices list is indexed with sequential numbers: we mimic it
  boot.panel.id <- replicate(R, sample(seq.indices, replace = TRUE)) # generate a matrix of new IDs - R times
  new.indices <- list() # generate the matrix of the new indices
  ind <- 1:length(unique.ids)
  for (r in 1:R){ # for each boot rep we generate a vector of indices with rownames equal to a new - and fake - ID
    new.indices[[r]] <- cbind(unlist(mapply(function(x,y) {
      names(panel.time.indices[[x]][[1]]) <- rep(y,length(panel.time.indices[[x]][[1]]))
      return(list(panel.time.indices[[x]][[1]]))
    }, boot.panel.id[,r], ind))) # return a fake ID (sequential number) as row name and the index referring to the true ID
  }
  return(new.indices)
}
# end of block bootstrap function #

# function to compute the weighting matrix #
weightM <- function(Y, X1, X2, Z1, Z2, betas, numR, SE = FALSE){
  k1 <- ncol(X1)
  N <- nrow(X1)
  R1t <- Y - X1 %*% betas[1 : k1, drop = FALSE]
  R2t <- Y - X2 %*% c(betas[1 : numR], betas[(k1 + 1) : length(betas), drop = FALSE]) # (fnum + snum + cnum + 1)
  u <- c(R1t, R2t) # alternative, still working
  Z <- as.matrix( bdiag(Z1, Z2)) # drop the collinear constant
  sigma.rs <- (t(u) %*% u)
  S <- sigma.rs[1] * ( ( t(Z) %*% Z) ) # /N
  if (SE == TRUE){
    dX <- rbind( cbind( X1, matrix(0, N, (ncol(X2) - numR) ) ),
                 cbind( X2[, 1 : numR], matrix(0 , N, (ncol(X1) - numR) ), X2[,(numR + 1) : ncol(X2)]) ) # generate a "quasi-block" matrix with common columns NON-BLOCK
    var.beta <- (1/N) * solve( ( t(dX) %*% Z ) %*% solve(S) %*% (t(Z) %*% dX) ) # compute varCovar matrix
    st.errors <- sqrt(diag(var.beta))
    return(st.errors)
  } else {
    W = solve(S)
    return(W)
  }
}
# end of weighting matrix function #

# function to print lateX table of results #
printProd <- function(mods, modnames = NULL, parnames = NULL, outfile = NULL, ptime = FALSE, nboot = FALSE){
  if (!is.null(outfile)) (sink(outfile)) # write on a text file
  numMods <- length(mods)
  numPars <- length(mods[[1]]@Estimates$pars)
  cat(paste('\\begin{tabular}{', paste(rep('c',(numMods*2+1)), collapse = ''),'}',
            '\\hline\\hline', sep = '')) # print tabular header
  nm <- '\n'
  obs <- '\nN'
  time <- '\nTime'
  boot <- '\nBootRep'
  for (m in 1:numMods){ # generate first and last row: names (methods or user-supplied) and observations
    if (is.null(modnames)){
      nm <- paste(nm, mods[[m]]@Model$method, sep = ' & & ')
    }else{
      nm <- paste(nm, modnames[m], sep = ' & & ')
    }
    obs <- paste(obs, length(mods[[m]]@Data$Y), sep = ' & & ')
    time <- paste(time, round(mods[[m]]@Model$elapsed.time[[1]], digits = 2), sep = ' & & ')
    boot <- paste(boot, mods[[m]]@Model$boot.repetitions, sep = ' & & ')
  }
  nm <- paste(nm, '\\\\\\hline')
  obs <- paste(obs, '\\\\\\hline\\hline')
  cat(nm)
  for (p in 1:numPars){ # generate the table body row by row: names (vars or user-supplied),
    if (is.null(parnames)){
      betas <- paste('\n', names(mods[[1]]@Estimates$pars)[p])
    }
    else{
      betas <- paste('\n', parnames[p])
    }
    sigmas <- '\n'
    blank <- '\n'
    for (m in 1:numMods){
      betas <- paste(betas, round(mods[[m]]@Estimates$pars[p],digits = 3), sep = ' & & ')
      sigma <- paste('(', round(mods[[m]]@Estimates$std.errors[p],digits = 3), ')', sep = '')
      sigmas <- paste(sigmas, sigma , sep = ' & & ')
      blank <- paste(blank, ' & ',  sep = '')
    }
    betas <- paste(betas, '\\\\')
    sigmas <- paste(sigmas, '\\\\')
    blank <- paste(blank, '\\\\')
    cat(betas)
    cat(sigmas)
    cat(blank)
  }
  cat(blank)
  if (ptime == TRUE) (cat(paste(time, '\\\\', sep = '')))
  if (nboot == TRUE) (cat(paste(boot, '\\\\', sep = '')))
  cat(obs)
  cat('\n\\end{tabular}')
  if (!is.null(outfile)) (sink())
}
# end of latex print table #

############ OLLEY-PAKES and LEVINSOHN-PETRIN ###############

# function to estimate OP and LP model #
prodestOP <- function(Y, fX, sX, pX, idvar, timevar, G = 3, A = 3, orth = F, R = 20, cX = NULL, opt = 'optim',
                      theta0 = NULL, seed = 123456, cluster = NULL, tol = 1e-100){
  set.seed(seed)
  Start = Sys.time() # start tracking time
  Y <- checkM(Y) # change all input to matrix
  fX <- checkM(fX)
  sX <- checkM(sX)
  pX <- checkM(pX)
  idvar <- checkM(idvar)
  timevar <- checkM(timevar)
  snum <- ncol(sX) # find the number of input variables
  fnum <- ncol(fX)
  if (!is.null(cX)) {cX <- checkM(cX); cnum <- ncol(cX)} else {cnum <- 0} # if is there any control, take it into account, else fix the number of controls to 0
  polyframe <- poly(sX,pX,degree=G,raw=!orth) # create (orthogonal / raw) polynomial of degree G
  mod <- cbind(fX,sX,pX,polyframe) # to make sure 1st degree variables come first (lm will drop the other ones from 1st-stage reg)
  regvars <- mod # replace NAs if there was any -> We change this line compared with the original file
  lag.sX = sX # generate sX lags
  for (i in 1:snum) {
    lag.sX[, i] = lagPanel(sX[, i], idvar = idvar, timevar = timevar)
  }
  if (!is.null(cX)) {  # generate the matrix of data
    data <- suppressWarnings(as.matrix(data.frame(state = sX, lag.sX = lag.sX, free = fX, cX = cX, Y = Y,
                                 idvar = idvar, timevar = timevar, regvars = regvars)))
  } else {
    data <- suppressWarnings(as.matrix(data.frame(state = sX, lag.sX = lag.sX, free = fX, Y = Y, idvar = idvar,
                                 timevar = timevar, regvars = regvars)))
  }
  betas <- finalOPLP(ind = TRUE, data = data, fnum = fnum, snum = snum, cnum = cnum, opt = opt,
                     theta0 = theta0, boot = FALSE, tol = tol, A = A) # generate the list of estimated betas
  boot.indices <- block.boot.resample(idvar, R) # generate a list: every element has different length (same IDs, different time occasions) and is a vector of new indices, whose rownames are the new IDs
  if (is.null(cluster)){
    nCores = NULL
    boot.betas <- matrix(unlist(lapply(boot.indices, finalOPLP, data = data, fnum = fnum, snum = snum, cnum = cnum,
                                       opt = opt, theta0 = theta0, boot = TRUE, tol = tol)), ncol = fnum+snum+cnum, byrow = TRUE) # use the indices and pass them to the final function (reshape the data)
  } else { # set up the cluster: send the lag Panel and the data.table libraries to clusters
    nCores = length(cluster)
    clusterEvalQ(cl = cluster, library(prodest))
    boot.betas <- matrix(unlist(parLapply(cl = cluster, boot.indices, finalOPLP, data = data,
                                          fnum = fnum, snum = snum, cnum = cnum,
                                          opt = opt, theta0 = theta0, boot = TRUE, tol = tol)), ncol = fnum+snum+cnum,
                         byrow = TRUE) # use the indices and pass them to the final function (reshape the data)
  }
  boot.errors <- apply(boot.betas,2,sd,na.rm=TRUE) # calculate standard deviations
  res.names <- c(colnames(fX, do.NULL = FALSE, prefix = 'fX'),
                 colnames(sX, do.NULL = FALSE, prefix = 'sX') ) # generate the list of names for results
  if (!is.null(cX)) {res.names <- c(res.names,colnames(cX, do.NULL = FALSE, prefix = 'cX'))}
  names(betas$betas) <- res.names # change results' names
  names(betas$FSbetas)[2 : length(betas$FSbetas)] <- c(res.names, rep(" ", (length(betas$FSbetas) - length(res.names) - 1))) # change 1st stage results' names
  names(boot.errors) <- res.names # change std.errors' names
  elapsedTime = Sys.time() - Start # total running time

  Model = list(method = 'OP', FSbetas = betas$FSbetas, boot.repetitions = R, elapsed.time = elapsedTime,
                          theta0 = theta0 , opt = opt, seed = seed, opt.outcome = betas$opt.outcome, nCores = nCores)

  Data = list(Y = Y, free = fX, state = sX, proxy = pX, control = cX, idvar = idvar, timevar = timevar,
                         FSresiduals = betas$FSresiduals)

  Estimates = list(pars = betas$betas, std.errors = boot.errors)
  return(Estimates)
}
# end of prodestOP #

# function to estimate OP and LP model #
prodestLP <- function(Y, fX, sX, pX, idvar, timevar, R = 20, cX = NULL, opt = 'optim',
                      theta0 = NULL, seed = 123456, cluster = NULL, tol = 1e-100){
  set.seed(seed)
  Start = Sys.time() # start tracking time
  Y <- checkM(Y) # change all input to matrix
  fX <- checkM(fX)
  sX <- checkM(sX)
  pX <- checkM(pX)
  idvar <- checkM(idvar)
  timevar <- checkM(timevar)
  snum <- ncol(sX) # find the number of input variables
  fnum <- ncol(fX)
  if (!is.null(cX)) {cX <- checkM(cX); cnum <- ncol(cX)} else {cnum <- 0} # if is there any control, take it into account, else fix the number of controls to 0
  polyframe <- data.frame(cbind(sX,pX)) # vars to be used in polynomial approximation
  mod <- model.matrix( ~.^2-1, data = polyframe) # generate the polynomial elements - this drops NAs
  mod <- mod[match(rownames(polyframe),rownames(mod)),] # replace NAs if there was any
  regvars <- cbind(fX, cX, mod, sX^2, pX^2) # generate a polynomial of the desired level
  lag.sX = sX # generate sX lags
  for (i in 1:snum) {
    lag.sX[, i] = lagPanel(sX[, i], idvar = idvar, timevar = timevar)
  }
  if (!is.null(cX)) {  # generate the matrix of data
    data <- suppressWarnings(as.matrix(data.frame(state = sX, lag.sX = lag.sX, free = fX, cX = cX, Y = Y,
                                 idvar = idvar, timevar = timevar, regvars = regvars)))
  } else {
    data <- suppressWarnings(as.matrix(data.frame(state = sX, lag.sX = lag.sX, free = fX, Y = Y, idvar = idvar,
                                 timevar = timevar, regvars = regvars)))
  }
  betas <- finalOPLP(ind = TRUE, data = data, fnum = fnum, snum = snum, cnum = cnum, opt = opt,
                     theta0 = theta0, boot = FALSE, tol = tol, A = A) # generate the list of estimated betas
  boot.indices <- block.boot.resample(idvar, R) # generate a list: every element has different length (same IDs, different time occasions) and is a vector of new indices, whose rownames are the new IDs
  if (is.null(cluster)){
    nCores = NULL
    boot.betas <- matrix(NA, R, (fnum + snum + cnum))
    for (i in 1:R){
      boot.betas[i,] <- finalOPLP(ind = boot.indices[[i]], data = data, fnum = fnum, snum = snum, cnum = cnum,
                                  opt = opt, theta0 = theta0, boot = TRUE, tol = tol)
    }
  } else { # set up the cluster: send the lag Panel and the data.table libraries to clusters
    nCores = length(cluster)
    clusterEvalQ(cl = cluster, library(prodest))
    boot.betas <- matrix(unlist(parLapply(cl = cluster, boot.indices, finalOPLP, data = data,
                                          fnum = fnum, snum = snum, cnum = cnum,
                                          opt = opt, theta0 = theta0, boot = TRUE, tol = tol)), ncol = fnum + snum + cnum,
                         byrow = TRUE) # use the indices and pass them to the final function (reshape the data)
  }
  boot.errors <- apply(boot.betas, 2, sd, na.rm = TRUE) # calculate standard deviations
  res.names <- c(colnames(fX, do.NULL = FALSE, prefix = 'fX'),
                 colnames(sX, do.NULL = FALSE, prefix = 'sX') ) # generate the list of names for results
  if (!is.null(cX)) {res.names <- c(res.names,colnames(cX, do.NULL = FALSE, prefix = 'cX'))}
  names(betas$betas) <- res.names # change results' names
  names(betas$FSbetas)[2 : length(betas$FSbetas)] <- c(res.names, rep(" ", (length(betas$FSbetas) - length(res.names) - 1))) # change 1st stage results' names
  names(boot.errors) <- res.names # change std.errors' names
  elapsedTime = Sys.time() - Start # total running time
  out <- new("prod",
             Model = list(method = 'LP', FSbetas = betas$FSbetas, boot.repetitions = R, elapsed.time = elapsedTime,
                          theta0 = theta0 , opt = opt, seed = seed, opt.outcome = betas$opt.outcome, nCores = nCores),
             Data = list(Y = Y, free = fX, state = sX, proxy = pX, control = cX, idvar = idvar, timevar = timevar,
                         FSresiduals = betas$FSresiduals),
             Estimates = list(pars = betas$betas, std.errors = boot.errors))
  return(out)
}
# end of prodestLP #

# function to estimate and to bootstrap OP / LP #
finalOPLP <- function(ind, data, fnum, snum, cnum, opt, theta0, boot, tol, A){
  if (sum(as.numeric(ind)) == length(ind)){ # if the ind variable is not always TRUE
    newid <- data[ind, 'idvar', drop = FALSE]
  } else {
    newid <- as.matrix(as.numeric(rownames(ind)))
    ind <- as.matrix(ind)
  }
  data <- data[ind,] # change the index according to bootstrapped indices
  first.stage <- lm(data[,'Y'] ~ data[,grepl('regvars', colnames(data))], na.action = na.exclude)
  fX <- data[,grepl('free', colnames(data)), drop = FALSE]
  phi <- fitted(first.stage) # generate the fitted values of the first stage
  beta.free <-  coef(first.stage)[2:(1 + fnum)]
  if (cnum != 0) { # save betas of cX if included
    beta.control <- coef(first.stage)[(2 + fnum):(1 + fnum + cnum)]
  } else {
    beta.control <- NULL
  }
  if (is.null(theta0)) { # if not specified, starting points are the first stage + normal noise
    theta0 <- coef(first.stage)[(2 + fnum):(1 + fnum + snum)]  + rnorm((snum), 0, 0.01)
  }
  phi <- phi - (fX %*% beta.free) # "clean" the phi from the effects of free vars
  newtime <- data[,'timevar', drop = FALSE]
  rownames(phi) <- NULL
  rownames(newtime) <- NULL
  lag.phi <- lagPanel(value = phi, idvar = newid, timevar = newtime) #  # lag fitted values
  res <- data[,'Y', drop = FALSE] - (fX %*% beta.free) # "clean" the outcome from the effects of free vars
  state = data[, grepl('state', colnames(data)), drop = FALSE]
  lag.sX = data[, grepl('lag.sX', colnames(data)), drop = FALSE]
  tmp.data <- model.frame(state ~ lag.sX + phi + lag.phi + res)
  if (opt == 'optim'){
    try.state <- try(optim(theta0, gOPLP, method = "BFGS", mX = tmp.data$state, mlX = tmp.data$lag.sX,
                           vphi = tmp.data$phi, vlag.phi = tmp.data$lag.phi, vres = tmp.data$res, stol = tol, A = A),
                           silent = TRUE)
    if (!inherits(try.state, "try-error")) {
      beta.state <- try.state$par
      opt.outcome <- try.state
    } else {
      beta.state <- matrix(NA,(snum),1)
      opt.outcome <- list(convergence = 99)
    } # error handling: if the optimization fails return missing values
  } else if (opt == 'DEoptim') {
    try.state <- try(DEoptim(fn = gOPLP, lower = c(theta0), upper = rep.int(1,length(theta0)),
                             mX = tmp.data$state, mlX = tmp.data$lag.sX,
                             vphi = tmp.data$phi, vlag.phi = tmp.data$lag.phi,
                             vres =  tmp.data$res, stol = tol, A = A, control = DEoptim.control(trace = FALSE)),
                     silent = TRUE)
    if (!inherits(try.state, "try-error")) {
      beta.state <- try.state$optim$bestmem
      opt.outcome <- try.state
    } else {
      beta.state <- matrix(NA,(snum), 1)
      opt.outcome <- list(convergence = 99)
    } # error handling: if the optimization fails return missing values
  } else if (opt == 'solnp') {
    try.state <- try(solnp(theta0, gOPLP, mX = tmp.data$state, mlX = tmp.data$lag.sX,
                           vphi = tmp.data$phi, vlag.phi = tmp.data$lag.phi,
                           vres =  tmp.data$res, stol = tol, A = A, control = list(trace = FALSE)), silent = TRUE)
    if (!inherits(try.state, "try-error")) {
      beta.state <- try.state$pars
      opt.outcome <- try.state
    } else {
      beta.state <- matrix(NA,(snum),1)
      opt.outcome <- list(convergence = 99)
    } # error handling: if the optimization fails return missing values
  }
  if (boot == FALSE){ # for the baseline estimation we want info on optimization, too
    return(list(betas = c(beta.free, beta.state, beta.control), opt.outcome = opt.outcome, FSbetas = coef(first.stage),
                FSresiduals = resid(first.stage)))
  } else{
    return(c(beta.free, beta.state, beta.control))
  }
}
# end of OP / LP final function #

# function to run the GMM estimation for OP and LP #
gOPLP <- function(vtheta, mX, mlX, vphi, vlag.phi, vres, stol, A){
  Omega <- vphi - mX %*% vtheta
  Omega_lag <- vlag.phi - mlX %*% vtheta
  Omega_lag_pol <- poly(Omega_lag,degree = A,raw = T) # create polynomial in omega for given degree
  Omega_lag_pol <- cbind(1, Omega_lag_pol)
  g_b <- solve(crossprod(Omega_lag_pol), tol = stol) %*% t(Omega_lag_pol) %*% Omega
  XI <- vres - (mX %*% vtheta) - (Omega_lag_pol %*% g_b)
  crit <- crossprod(XI)
  return(crit)
}
# end of GMM OPLP #
